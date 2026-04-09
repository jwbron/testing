"""Modular microgpt library.

A refactoring of Karpathy's microgpt into composable components:
config models, scalar autograd, NN primitives, tokenizer, model init/forward,
training, and inference.
"""

from __future__ import annotations

import math
import os
import random

from pydantic import BaseModel, computed_field

# ---------------------------------------------------------------------------
# 1. Config models
# ---------------------------------------------------------------------------


class GPTConfig(BaseModel):
    n_layer: int = 1
    n_embd: int = 16
    block_size: int = 16
    n_head: int = 4
    vocab_size: int = 0  # set at runtime

    @computed_field  # type: ignore[prop-decorator]
    @property
    def head_dim(self) -> int:
        return self.n_embd // self.n_head


class AdamConfig(BaseModel):
    learning_rate: float = 0.01
    beta1: float = 0.85
    beta2: float = 0.99
    eps: float = 1e-8


class SampleConfig(BaseModel):
    temperature: float = 0.5
    max_tokens: int = 16
    num_samples: int = 20


# ---------------------------------------------------------------------------
# 2. Scalar autograd
# ---------------------------------------------------------------------------


class Scalar:
    """Scalar autograd node for reverse-mode automatic differentiation."""

    __slots__ = ("data", "grad", "_children", "_local_grads")

    def __init__(
        self,
        data: float,
        children: tuple[Scalar, ...] = (),
        local_grads: tuple[float, ...] = (),
    ) -> None:
        self.data = data
        self.grad = 0.0
        self._children = children
        self._local_grads = local_grads

    # --- arithmetic operators ---

    def __add__(self, other: Scalar | float) -> Scalar:
        other = other if isinstance(other, Scalar) else Scalar(float(other))
        return Scalar(self.data + other.data, (self, other), (1.0, 1.0))

    def __mul__(self, other: Scalar | float) -> Scalar:
        other = other if isinstance(other, Scalar) else Scalar(float(other))
        return Scalar(self.data * other.data, (self, other), (other.data, self.data))

    def __pow__(self, other: float) -> Scalar:
        return Scalar(
            self.data**other,
            (self,),
            (other * self.data ** (other - 1),),
        )

    def __neg__(self) -> Scalar:
        return self * -1.0

    def __sub__(self, other: Scalar | float) -> Scalar:
        if isinstance(other, Scalar):
            return self + (-other)
        return self + Scalar(-float(other))

    def __truediv__(self, other: Scalar | float) -> Scalar:
        if isinstance(other, Scalar):
            return self * other**-1.0
        return self * (1.0 / float(other))

    # --- reflected operators ---

    def __radd__(self, other: float) -> Scalar:
        return self + other

    def __rmul__(self, other: float) -> Scalar:
        return self * other

    def __rsub__(self, other: float) -> Scalar:
        return Scalar(float(other)) + (-self)

    def __rtruediv__(self, other: float) -> Scalar:
        return Scalar(float(other)) * self**-1.0

    # --- unary math ---

    def log(self) -> Scalar:
        return Scalar(math.log(self.data), (self,), (1.0 / self.data,))

    def exp(self) -> Scalar:
        return Scalar(math.exp(self.data), (self,), (math.exp(self.data),))

    def relu(self) -> Scalar:
        return Scalar(max(0.0, self.data), (self,), (float(self.data > 0),))

    # --- backward ---

    def backward(self) -> None:
        topo: list[Scalar] = []
        visited: set[int] = set()

        def build_topo(v: Scalar) -> None:
            if id(v) not in visited:
                visited.add(id(v))
                for child in v._children:
                    build_topo(child)
                topo.append(v)

        build_topo(self)
        self.grad = 1.0
        for v in reversed(topo):
            for child, local_grad in zip(v._children, v._local_grads):
                child.grad += local_grad * v.grad


# ---------------------------------------------------------------------------
# 3. NN primitives
# ---------------------------------------------------------------------------


def linear(x: list[Scalar], w: list[list[Scalar]]) -> list[Scalar]:
    """Matrix-vector multiply: w @ x."""
    result: list[Scalar] = []
    for wo in w:
        acc = Scalar(0.0)
        for wi, xi in zip(wo, x):
            acc = acc + wi * xi
        result.append(acc)
    return result


def softmax(logits: list[Scalar]) -> list[Scalar]:
    """Numerically stable softmax."""
    max_val = max(val.data for val in logits)
    exps = [(val - max_val).exp() for val in logits]
    total = exps[0]
    for e in exps[1:]:
        total = total + e
    return [e / total for e in exps]


def rmsnorm(x: list[Scalar]) -> list[Scalar]:
    """Root-mean-square layer normalization."""
    ms = Scalar(0.0)
    for xi in x:
        ms = ms + xi * xi
    ms = ms / float(len(x))
    scale = (ms + 1e-5) ** -0.5
    return [xi * scale for xi in x]


# ---------------------------------------------------------------------------
# 4. Tokenizer
# ---------------------------------------------------------------------------


def build_vocab(docs: list[str]) -> tuple[list[str], int]:
    """Extract unique sorted characters from docs. Returns (vocab, bos_id)."""
    vocab = sorted(set("".join(docs)))
    bos = len(vocab)
    return vocab, bos


def encode(text: str, vocab: list[str], bos: int) -> list[int]:
    """Encode text as token IDs, wrapped with BOS tokens."""
    result: list[int] = [bos]
    for ch in text:
        if ch not in vocab:
            raise ValueError(f"Unknown character: {ch!r}")
        result.append(vocab.index(ch))
    result.append(bos)
    return result


def decode(token_ids: list[int], vocab: list[str], bos: int) -> str:
    """Decode token IDs back to text, stripping BOS tokens."""
    return "".join(vocab[t] for t in token_ids if t != bos)


def load_dataset(path: str, url: str | None = None) -> list[str]:
    """Load dataset from file, downloading from url if needed."""
    if not os.path.exists(path) and url is not None:
        import urllib.request

        urllib.request.urlretrieve(url, path)
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


# ---------------------------------------------------------------------------
# 5. Model init and forward pass
# ---------------------------------------------------------------------------

StateDict = dict[str, list[list[Scalar]]]


def _matrix(
    nout: int,
    nin: int,
    std: float = 0.08,
    rng: random.Random | None = None,
) -> list[list[Scalar]]:
    """Create a random weight matrix."""
    r = rng or random.Random()
    return [[Scalar(r.gauss(0, std)) for _ in range(nin)] for _ in range(nout)]


def init_state_dict(config: GPTConfig, rng: random.Random | None = None) -> StateDict:
    """Initialize all model weights."""
    sd: StateDict = {
        "wte": _matrix(config.vocab_size, config.n_embd, rng=rng),
        "wpe": _matrix(config.block_size, config.n_embd, rng=rng),
        "lm_head": _matrix(config.vocab_size, config.n_embd, rng=rng),
    }
    for i in range(config.n_layer):
        sd[f"layer{i}.attn_wq"] = _matrix(config.n_embd, config.n_embd, rng=rng)
        sd[f"layer{i}.attn_wk"] = _matrix(config.n_embd, config.n_embd, rng=rng)
        sd[f"layer{i}.attn_wv"] = _matrix(config.n_embd, config.n_embd, rng=rng)
        sd[f"layer{i}.attn_wo"] = _matrix(config.n_embd, config.n_embd, rng=rng)
        sd[f"layer{i}.mlp_fc1"] = _matrix(4 * config.n_embd, config.n_embd, rng=rng)
        sd[f"layer{i}.mlp_fc2"] = _matrix(config.n_embd, 4 * config.n_embd, rng=rng)
    return sd


def gpt(
    token_id: int,
    pos_id: int,
    keys: list[list[list[Scalar]]],
    values: list[list[list[Scalar]]],
    config: GPTConfig,
    state_dict: StateDict,
) -> list[Scalar]:
    """Single-token forward pass through the GPT model."""
    tok_emb = state_dict["wte"][token_id]
    pos_emb = state_dict["wpe"][pos_id]
    x = [t + p for t, p in zip(tok_emb, pos_emb)]
    x = rmsnorm(x)

    for li in range(config.n_layer):
        x_residual = x
        x = rmsnorm(x)
        q = linear(x, state_dict[f"layer{li}.attn_wq"])
        k = linear(x, state_dict[f"layer{li}.attn_wk"])
        v = linear(x, state_dict[f"layer{li}.attn_wv"])
        keys[li].append(k)
        values[li].append(v)

        x_attn: list[Scalar] = []
        for h in range(config.n_head):
            hs = h * config.head_dim
            q_h = q[hs : hs + config.head_dim]
            k_h = [ki[hs : hs + config.head_dim] for ki in keys[li]]
            v_h = [vi[hs : hs + config.head_dim] for vi in values[li]]
            attn_logits: list[Scalar] = []
            for t in range(len(k_h)):
                acc = Scalar(0.0)
                for j in range(config.head_dim):
                    acc = acc + q_h[j] * k_h[t][j]
                attn_logits.append(acc / config.head_dim**0.5)
            attn_weights = softmax(attn_logits)
            for j in range(config.head_dim):
                acc = Scalar(0.0)
                for t in range(len(v_h)):
                    acc = acc + attn_weights[t] * v_h[t][j]
                x_attn.append(acc)

        x = linear(x_attn, state_dict[f"layer{li}.attn_wo"])
        x = [a + b for a, b in zip(x, x_residual)]

        x_residual = x
        x = rmsnorm(x)
        x = linear(x, state_dict[f"layer{li}.mlp_fc1"])
        x = [xi.relu() for xi in x]
        x = linear(x, state_dict[f"layer{li}.mlp_fc2"])
        x = [a + b for a, b in zip(x, x_residual)]

    logits = linear(x, state_dict["lm_head"])
    return logits


# ---------------------------------------------------------------------------
# 6. Training and inference
# ---------------------------------------------------------------------------


def adam_step(
    params: list[Scalar],
    m: list[float],
    v: list[float],
    step: int,
    config: AdamConfig,
    lr_scale: float = 1.0,
) -> None:
    """One step of Adam optimizer (in-place)."""
    for i, p in enumerate(params):
        m[i] = config.beta1 * m[i] + (1 - config.beta1) * p.grad
        v[i] = config.beta2 * v[i] + (1 - config.beta2) * p.grad**2
        m_hat = m[i] / (1 - config.beta1 ** (step + 1))
        v_hat = v[i] / (1 - config.beta2 ** (step + 1))
        p.data -= lr_scale * config.learning_rate * m_hat / (v_hat**0.5 + config.eps)
        p.grad = 0.0


def _get_params(state_dict: StateDict) -> list[Scalar]:
    """Flatten all parameters from state dict."""
    return [p for mat in state_dict.values() for row in mat for p in row]


def train(
    docs: list[str],
    config: GPTConfig,
    adam_config: AdamConfig,
    num_steps: int,
    rng: random.Random | None = None,
) -> tuple[StateDict, list[str], int]:
    """Train a GPT model on the given documents."""
    r = rng or random.Random(42)
    vocab, bos = build_vocab(docs)
    config = config.model_copy(update={"vocab_size": len(vocab) + 1})

    state_dict = init_state_dict(config, rng=r)
    params = _get_params(state_dict)
    m_buf = [0.0] * len(params)
    v_buf = [0.0] * len(params)

    shuffled_docs = list(docs)
    r.shuffle(shuffled_docs)

    for step in range(num_steps):
        doc = shuffled_docs[step % len(shuffled_docs)]
        tokens = encode(doc, vocab, bos)
        n = min(config.block_size, len(tokens) - 1)

        kv_keys: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        kv_values: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        losses: list[Scalar] = []

        for pos_id in range(n):
            token_id, target_id = tokens[pos_id], tokens[pos_id + 1]
            logits = gpt(token_id, pos_id, kv_keys, kv_values, config, state_dict)
            probs = softmax(logits)
            loss_t = -probs[target_id].log()
            losses.append(loss_t)

        loss_acc = Scalar(0.0)
        for l_t in losses:
            loss_acc = loss_acc + l_t
        loss = loss_acc / float(n)
        loss.backward()

        lr_scale = 1.0 - step / num_steps
        adam_step(params, m_buf, v_buf, step, adam_config, lr_scale=lr_scale)

    return state_dict, vocab, bos


def sample(
    state_dict: StateDict,
    vocab: list[str],
    bos: int,
    config: GPTConfig,
    sample_config: SampleConfig,
    rng: random.Random | None = None,
) -> list[str]:
    """Generate text samples from a trained model."""
    r = rng or random.Random()
    results: list[str] = []

    for _ in range(sample_config.num_samples):
        kv_keys: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        kv_values: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        token_id = bos
        sample_tokens: list[int] = []

        for pos_id in range(sample_config.max_tokens):
            logits = gpt(token_id, pos_id, kv_keys, kv_values, config, state_dict)
            scaled = [lg / sample_config.temperature for lg in logits]
            probs = softmax(scaled)
            token_id = r.choices(
                range(config.vocab_size),
                weights=[p.data for p in probs],
            )[0]
            if token_id == bos:
                break
            sample_tokens.append(token_id)

        results.append(decode(sample_tokens, vocab, bos))

    return results
