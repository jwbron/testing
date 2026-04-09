"""Comprehensive tests for the microgpt challenge module."""

from __future__ import annotations

import math
import random

import pytest

from challenges.microgpt import (
    AdamConfig,
    GPTConfig,
    SampleConfig,
    Scalar,
    _get_params,
    _matrix,
    adam_step,
    build_vocab,
    decode,
    encode,
    gpt,
    init_state_dict,
    linear,
    rmsnorm,
    sample,
    softmax,
    train,
)

# ---------------------------------------------------------------------------
# Scalar autograd tests
# ---------------------------------------------------------------------------


class TestScalarAdd:
    """Tests for Scalar addition and gradient propagation."""

    def test_scalar_add(self) -> None:
        a = Scalar(2.0)
        b = Scalar(3.0)
        c = a + b
        c.backward()
        assert c.data == pytest.approx(5.0)
        assert a.grad == pytest.approx(1.0)
        assert b.grad == pytest.approx(1.0)

    def test_scalar_add_float_right(self) -> None:
        a = Scalar(2.0)
        c = a + 3.0
        c.backward()
        assert c.data == pytest.approx(5.0)
        assert a.grad == pytest.approx(1.0)

    def test_scalar_radd(self) -> None:
        a = Scalar(2.0)
        c = 3.0 + a
        c.backward()
        assert c.data == pytest.approx(5.0)
        assert a.grad == pytest.approx(1.0)


class TestScalarMul:
    """Tests for Scalar multiplication and gradient propagation."""

    def test_scalar_mul(self) -> None:
        a = Scalar(3.0)
        b = Scalar(4.0)
        c = a * b
        c.backward()
        assert c.data == pytest.approx(12.0)
        assert a.grad == pytest.approx(4.0)
        assert b.grad == pytest.approx(3.0)

    def test_scalar_mul_float_right(self) -> None:
        a = Scalar(3.0)
        c = a * 4.0
        c.backward()
        assert c.data == pytest.approx(12.0)
        assert a.grad == pytest.approx(4.0)

    def test_scalar_rmul(self) -> None:
        a = Scalar(3.0)
        c = 4.0 * a
        c.backward()
        assert c.data == pytest.approx(12.0)
        assert a.grad == pytest.approx(4.0)


class TestScalarPow:
    """Tests for Scalar power operation and gradient."""

    def test_scalar_pow(self) -> None:
        a = Scalar(2.0)
        c = a**3
        c.backward()
        assert c.data == pytest.approx(8.0)
        assert a.grad == pytest.approx(12.0)  # 3 * 2^2

    def test_scalar_pow_square(self) -> None:
        a = Scalar(5.0)
        c = a**2
        c.backward()
        assert c.data == pytest.approx(25.0)
        assert a.grad == pytest.approx(10.0)  # 2 * 5

    def test_scalar_pow_negative_exponent(self) -> None:
        a = Scalar(2.0)
        c = a**-1
        c.backward()
        assert c.data == pytest.approx(0.5)
        assert a.grad == pytest.approx(-0.25)  # -1 * 2^-2


class TestScalarRelu:
    """Tests for Scalar relu."""

    def test_scalar_relu_positive(self) -> None:
        a = Scalar(3.0)
        c = a.relu()
        c.backward()
        assert c.data == pytest.approx(3.0)
        assert a.grad == pytest.approx(1.0)

    def test_scalar_relu_negative(self) -> None:
        a = Scalar(-3.0)
        c = a.relu()
        c.backward()
        assert c.data == pytest.approx(0.0)
        assert a.grad == pytest.approx(0.0)

    def test_scalar_relu_zero(self) -> None:
        a = Scalar(0.0)
        c = a.relu()
        c.backward()
        assert c.data == pytest.approx(0.0)
        assert a.grad == pytest.approx(0.0)


class TestScalarLog:
    """Tests for Scalar log."""

    def test_scalar_log(self) -> None:
        a = Scalar(1.0)
        c = a.log()
        c.backward()
        assert c.data == pytest.approx(0.0)
        assert a.grad == pytest.approx(1.0)

    def test_scalar_log_e(self) -> None:
        a = Scalar(math.e)
        c = a.log()
        c.backward()
        assert c.data == pytest.approx(1.0)
        assert a.grad == pytest.approx(1.0 / math.e)


class TestScalarExp:
    """Tests for Scalar exp."""

    def test_scalar_exp(self) -> None:
        a = Scalar(0.0)
        c = a.exp()
        c.backward()
        assert c.data == pytest.approx(1.0)
        assert a.grad == pytest.approx(1.0)

    def test_scalar_exp_one(self) -> None:
        a = Scalar(1.0)
        c = a.exp()
        c.backward()
        assert c.data == pytest.approx(math.e)
        assert a.grad == pytest.approx(math.e)


class TestScalarSub:
    """Tests for Scalar subtraction."""

    def test_scalar_sub(self) -> None:
        a = Scalar(5.0)
        b = Scalar(3.0)
        c = a - b
        c.backward()
        assert c.data == pytest.approx(2.0)
        assert a.grad == pytest.approx(1.0)
        assert b.grad == pytest.approx(-1.0)

    def test_scalar_sub_float(self) -> None:
        a = Scalar(5.0)
        c = a - 3.0
        c.backward()
        assert c.data == pytest.approx(2.0)
        assert a.grad == pytest.approx(1.0)

    def test_scalar_rsub(self) -> None:
        a = Scalar(3.0)
        c = 5.0 - a
        c.backward()
        assert c.data == pytest.approx(2.0)
        assert a.grad == pytest.approx(-1.0)


class TestScalarDiv:
    """Tests for Scalar division."""

    def test_scalar_division(self) -> None:
        a = Scalar(6.0)
        b = Scalar(3.0)
        c = a / b
        c.backward()
        assert c.data == pytest.approx(2.0)
        # dc/da = 1/b = 1/3
        assert a.grad == pytest.approx(1.0 / 3.0)
        # dc/db = -a/b^2 = -6/9 = -2/3
        assert b.grad == pytest.approx(-2.0 / 3.0)

    def test_scalar_div_float(self) -> None:
        a = Scalar(6.0)
        c = a / 3.0
        c.backward()
        assert c.data == pytest.approx(2.0)
        assert a.grad == pytest.approx(1.0 / 3.0)

    def test_scalar_rtruediv(self) -> None:
        a = Scalar(2.0)
        c = 6.0 / a
        c.backward()
        assert c.data == pytest.approx(3.0)
        # d(6/a)/da = -6/a^2 = -6/4 = -1.5
        assert a.grad == pytest.approx(-1.5)


class TestScalarNeg:
    """Tests for Scalar negation."""

    def test_scalar_neg(self) -> None:
        a = Scalar(3.0)
        c = -a
        c.backward()
        assert c.data == pytest.approx(-3.0)
        assert a.grad == pytest.approx(-1.0)


class TestScalarComposite:
    """Tests for composite Scalar operations."""

    def test_scalar_composite_backward(self) -> None:
        """Multi-op chain: (a * b + c).relu()."""
        a = Scalar(2.0)
        b = Scalar(3.0)
        c = Scalar(-10.0)
        d = (a * b + c).relu()
        d.backward()
        # a*b + c = 6 - 10 = -4, relu(-4) = 0
        assert d.data == pytest.approx(0.0)
        # All grads zero because relu kills gradient
        assert a.grad == pytest.approx(0.0)
        assert b.grad == pytest.approx(0.0)
        assert c.grad == pytest.approx(0.0)

    def test_scalar_composite_positive_relu(self) -> None:
        """Multi-op chain: (a * b + c).relu() with positive result."""
        a = Scalar(2.0)
        b = Scalar(3.0)
        c = Scalar(1.0)
        d = (a * b + c).relu()
        d.backward()
        # a*b + c = 7, relu(7) = 7
        assert d.data == pytest.approx(7.0)
        assert a.grad == pytest.approx(3.0)  # d(a*b)/da * 1 = b
        assert b.grad == pytest.approx(2.0)  # d(a*b)/db * 1 = a
        assert c.grad == pytest.approx(1.0)

    def test_scalar_same_variable_used_twice(self) -> None:
        """Variable used multiple times: a * a = a^2."""
        a = Scalar(3.0)
        c = a * a
        c.backward()
        assert c.data == pytest.approx(9.0)
        # Gradient should accumulate: grad = 3 + 3 = 6 = 2*a
        assert a.grad == pytest.approx(6.0)

    def test_scalar_chain_exp_log(self) -> None:
        """exp(log(x)) should give back x with gradient 1."""
        a = Scalar(5.0)
        c = a.log().exp()
        c.backward()
        assert c.data == pytest.approx(5.0)
        assert a.grad == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# NN primitive tests
# ---------------------------------------------------------------------------


class TestLinear:
    """Tests for linear (matrix-vector multiply)."""

    def test_linear_identity(self) -> None:
        """Identity matrix should return the input vector."""
        n = 3
        x = [Scalar(float(i + 1)) for i in range(n)]
        w = [[Scalar(1.0 if i == j else 0.0) for j in range(n)] for i in range(n)]
        result = linear(x, w)
        assert len(result) == n
        for i in range(n):
            assert result[i].data == pytest.approx(float(i + 1))

    def test_linear_output_shape(self) -> None:
        """Output shape should match number of rows in weight matrix."""
        x = [Scalar(1.0), Scalar(2.0)]
        w = [
            [Scalar(1.0), Scalar(0.0)],
            [Scalar(0.0), Scalar(1.0)],
            [Scalar(1.0), Scalar(1.0)],
        ]
        result = linear(x, w)
        assert len(result) == 3

    def test_linear_known_product(self) -> None:
        """Known matrix-vector product."""
        x = [Scalar(1.0), Scalar(2.0)]
        w = [[Scalar(3.0), Scalar(4.0)]]  # [3*1 + 4*2] = [11]
        result = linear(x, w)
        assert len(result) == 1
        assert result[0].data == pytest.approx(11.0)


class TestSoftmax:
    """Tests for numerically stable softmax."""

    def test_softmax_uniform(self) -> None:
        """Equal logits should give uniform distribution."""
        logits = [Scalar(1.0), Scalar(1.0), Scalar(1.0)]
        probs = softmax(logits)
        for p in probs:
            assert p.data == pytest.approx(1.0 / 3.0, abs=1e-6)

    def test_softmax_sums_to_one(self) -> None:
        """Softmax output should sum to 1."""
        logits = [Scalar(1.0), Scalar(2.0), Scalar(3.0)]
        probs = softmax(logits)
        total = sum(p.data for p in probs)
        assert total == pytest.approx(1.0, abs=1e-6)

    def test_softmax_numerical_stability(self) -> None:
        """Large logits should not produce NaN or inf."""
        logits = [Scalar(1000.0), Scalar(1001.0), Scalar(1002.0)]
        probs = softmax(logits)
        for p in probs:
            assert math.isfinite(p.data)
        total = sum(p.data for p in probs)
        assert total == pytest.approx(1.0, abs=1e-6)

    def test_softmax_peak_at_max(self) -> None:
        """Largest logit should get highest probability."""
        logits = [Scalar(1.0), Scalar(5.0), Scalar(2.0)]
        probs = softmax(logits)
        assert probs[1].data > probs[0].data
        assert probs[1].data > probs[2].data

    def test_softmax_single_element(self) -> None:
        """Single element softmax should be 1.0."""
        probs = softmax([Scalar(42.0)])
        assert probs[0].data == pytest.approx(1.0)


class TestRmsnorm:
    """Tests for root-mean-square normalization."""

    def test_rmsnorm_unit_scale(self) -> None:
        """Output should have approximately unit RMS."""
        x = [Scalar(3.0), Scalar(4.0)]
        result = rmsnorm(x)
        rms = math.sqrt(sum(r.data**2 for r in result) / len(result))
        assert rms == pytest.approx(1.0, abs=1e-2)

    def test_rmsnorm_preserves_sign(self) -> None:
        """RMS normalization should preserve the sign of each element."""
        x = [Scalar(3.0), Scalar(-4.0), Scalar(5.0)]
        result = rmsnorm(x)
        assert result[0].data > 0
        assert result[1].data < 0
        assert result[2].data > 0

    def test_rmsnorm_output_length(self) -> None:
        """Output should have same length as input."""
        x = [Scalar(float(i)) for i in range(5)]
        result = rmsnorm(x)
        assert len(result) == 5


# ---------------------------------------------------------------------------
# Tokenizer tests
# ---------------------------------------------------------------------------


class TestBuildVocab:
    """Tests for build_vocab function."""

    def test_build_vocab(self) -> None:
        docs = ["abc", "bca"]
        vocab, bos = build_vocab(docs)
        assert vocab == ["a", "b", "c"]
        assert bos == 3

    def test_build_vocab_single_doc(self) -> None:
        vocab, bos = build_vocab(["hello"])
        assert vocab == ["e", "h", "l", "o"]
        assert bos == 4

    def test_build_vocab_deduplicates(self) -> None:
        vocab, bos = build_vocab(["aaa", "bbb"])
        assert vocab == ["a", "b"]
        assert bos == 2


class TestEncode:
    """Tests for encode function."""

    def test_encode_decode_roundtrip(self) -> None:
        docs = ["hello", "world"]
        vocab, bos = build_vocab(docs)
        for doc in docs:
            encoded = encode(doc, vocab, bos)
            decoded = decode(encoded, vocab, bos)
            assert decoded == doc

    def test_encode_wraps_with_bos(self) -> None:
        vocab = ["a", "b", "c"]
        bos = 3
        tokens = encode("abc", vocab, bos)
        assert tokens[0] == bos
        assert tokens[-1] == bos
        assert len(tokens) == 5  # BOS + 3 chars + BOS

    def test_encode_unknown_char_raises(self) -> None:
        vocab = ["a", "b"]
        bos = 2
        with pytest.raises(ValueError, match="Unknown character"):
            encode("z", vocab, bos)


class TestDecode:
    """Tests for decode function."""

    def test_decode_strips_bos(self) -> None:
        vocab = ["a", "b", "c"]
        bos = 3
        token_ids = [bos, 0, 1, 2, bos]
        assert decode(token_ids, vocab, bos) == "abc"

    def test_decode_empty(self) -> None:
        vocab = ["a"]
        bos = 1
        assert decode([], vocab, bos) == ""

    def test_decode_only_bos(self) -> None:
        vocab = ["a"]
        bos = 1
        assert decode([bos, bos], vocab, bos) == ""


# ---------------------------------------------------------------------------
# Config model tests
# ---------------------------------------------------------------------------


class TestConfigs:
    """Tests for Pydantic config models."""

    def test_gpt_config_defaults(self) -> None:
        config = GPTConfig()
        assert config.n_layer == 1
        assert config.n_embd == 16
        assert config.block_size == 16
        assert config.n_head == 4
        assert config.head_dim == 4

    def test_gpt_config_head_dim_computed(self) -> None:
        config = GPTConfig(n_embd=8, n_head=2)
        assert config.head_dim == 4

    def test_adam_config_defaults(self) -> None:
        config = AdamConfig()
        assert config.learning_rate == 0.01
        assert config.beta1 == 0.85
        assert config.beta2 == 0.99
        assert config.eps == 1e-8

    def test_sample_config_defaults(self) -> None:
        config = SampleConfig()
        assert config.temperature == 0.5
        assert config.max_tokens == 16
        assert config.num_samples == 20

    def test_gpt_config_model_copy(self) -> None:
        """Config model_copy should update vocab_size (used in train)."""
        config = GPTConfig()
        new = config.model_copy(update={"vocab_size": 10})
        assert new.vocab_size == 10
        assert config.vocab_size == 0  # original unchanged


# ---------------------------------------------------------------------------
# Model init and forward tests
# ---------------------------------------------------------------------------


class TestInitStateDict:
    """Tests for init_state_dict function."""

    def test_init_state_dict_keys(self) -> None:
        config = GPTConfig(vocab_size=5, n_layer=2, n_embd=4, n_head=2)
        sd = init_state_dict(config)
        assert "wte" in sd
        assert "wpe" in sd
        assert "lm_head" in sd
        for i in range(2):
            assert f"layer{i}.attn_wq" in sd
            assert f"layer{i}.attn_wk" in sd
            assert f"layer{i}.attn_wv" in sd
            assert f"layer{i}.attn_wo" in sd
            assert f"layer{i}.mlp_fc1" in sd
            assert f"layer{i}.mlp_fc2" in sd

    def test_init_state_dict_shapes(self) -> None:
        config = GPTConfig(vocab_size=5, n_layer=1, n_embd=4, n_head=2)
        sd = init_state_dict(config)
        assert len(sd["wte"]) == 5
        assert len(sd["wte"][0]) == 4
        assert len(sd["wpe"]) == config.block_size
        assert len(sd["lm_head"]) == 5
        assert len(sd["layer0.mlp_fc1"]) == 16  # 4 * n_embd
        assert len(sd["layer0.mlp_fc2"]) == 4

    def test_init_state_dict_deterministic(self) -> None:
        config = GPTConfig(vocab_size=5, n_layer=1, n_embd=4, n_head=2)
        sd1 = init_state_dict(config, rng=random.Random(42))
        sd2 = init_state_dict(config, rng=random.Random(42))
        for key in sd1:
            for i in range(len(sd1[key])):
                for j in range(len(sd1[key][i])):
                    assert sd1[key][i][j].data == sd2[key][i][j].data


class TestGPT:
    """Tests for the GPT forward pass."""

    def _tiny_config(self) -> GPTConfig:
        return GPTConfig(n_embd=4, n_head=2, block_size=4, n_layer=1, vocab_size=5)

    def test_gpt_output_shape(self) -> None:
        config = self._tiny_config()
        sd = init_state_dict(config, rng=random.Random(42))
        keys: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        values: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        logits = gpt(0, 0, keys, values, config, sd)
        assert len(logits) == config.vocab_size

    def test_gpt_deterministic(self) -> None:
        config = self._tiny_config()
        sd = init_state_dict(config, rng=random.Random(42))

        keys1: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        vals1: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        logits1 = gpt(0, 0, keys1, vals1, config, sd)

        # Reset grads for second call
        for p in _get_params(sd):
            p.grad = 0.0

        keys2: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        vals2: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        logits2 = gpt(0, 0, keys2, vals2, config, sd)

        for l1, l2 in zip(logits1, logits2):
            assert l1.data == pytest.approx(l2.data)

    def test_gpt_returns_finite_values(self) -> None:
        config = self._tiny_config()
        sd = init_state_dict(config, rng=random.Random(42))
        keys: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        values: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        logits = gpt(0, 0, keys, values, config, sd)
        for logit in logits:
            assert math.isfinite(logit.data)

    def test_gpt_kv_cache_grows(self) -> None:
        """KV cache should accumulate entries for each position."""
        config = self._tiny_config()
        sd = init_state_dict(config, rng=random.Random(42))
        keys: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        values: list[list[list[Scalar]]] = [[] for _ in range(config.n_layer)]
        gpt(0, 0, keys, values, config, sd)
        assert len(keys[0]) == 1
        gpt(1, 1, keys, values, config, sd)
        assert len(keys[0]) == 2


# ---------------------------------------------------------------------------
# Training and inference tests
# ---------------------------------------------------------------------------


class TestAdamStep:
    """Tests for adam_step optimizer."""

    def test_adam_step_updates_params(self) -> None:
        params = [Scalar(1.0)]
        params[0].grad = 1.0
        m = [0.0]
        v = [0.0]
        config = AdamConfig()
        adam_step(params, m, v, 0, config)
        # Param should have changed
        assert params[0].data != 1.0
        # Grad should be zeroed
        assert params[0].grad == 0.0

    def test_adam_step_decreases_with_positive_grad(self) -> None:
        """Positive gradient should decrease the parameter."""
        params = [Scalar(1.0)]
        params[0].grad = 1.0
        m = [0.0]
        v = [0.0]
        config = AdamConfig()
        adam_step(params, m, v, 0, config)
        assert params[0].data < 1.0

    def test_adam_step_with_lr_scale(self) -> None:
        """lr_scale should affect the step size."""
        p1 = [Scalar(1.0)]
        p1[0].grad = 1.0
        m1 = [0.0]
        v1 = [0.0]
        config = AdamConfig()
        adam_step(p1, m1, v1, 0, config, lr_scale=1.0)

        p2 = [Scalar(1.0)]
        p2[0].grad = 1.0
        m2 = [0.0]
        v2 = [0.0]
        adam_step(p2, m2, v2, 0, config, lr_scale=0.5)

        # Smaller lr_scale should result in smaller change
        change1 = abs(p1[0].data - 1.0)
        change2 = abs(p2[0].data - 1.0)
        assert change2 < change1


class TestTrain:
    """Tests for the training function."""

    def test_training_loss_decreases(self) -> None:
        """Training loss should decrease over multiple steps on tiny data."""
        docs = ["ab", "ba", "aa", "bb"]
        config = GPTConfig(n_embd=4, n_head=2, block_size=4, n_layer=1)
        adam_config = AdamConfig(learning_rate=0.01)
        rng = random.Random(42)

        # Compute initial loss
        vocab, bos = build_vocab(docs)
        cfg = config.model_copy(update={"vocab_size": len(vocab) + 1})
        sd = init_state_dict(cfg, rng=random.Random(42))

        doc = docs[0]
        tokens = encode(doc, vocab, bos)
        n = min(cfg.block_size, len(tokens) - 1)
        kv_keys: list[list[list[Scalar]]] = [[] for _ in range(cfg.n_layer)]
        kv_values: list[list[list[Scalar]]] = [[] for _ in range(cfg.n_layer)]
        losses: list[Scalar] = []
        for pos_id in range(n):
            token_id, target_id = tokens[pos_id], tokens[pos_id + 1]
            logits = gpt(token_id, pos_id, kv_keys, kv_values, cfg, sd)
            probs = softmax(logits)
            loss_t = -probs[target_id].log()
            losses.append(loss_t)
        initial_loss_acc = Scalar(0.0)
        for l_t in losses:
            initial_loss_acc = initial_loss_acc + l_t
        initial_loss = initial_loss_acc.data / n

        # Train for some steps
        trained_sd, trained_vocab, trained_bos = train(
            docs, config, adam_config, num_steps=50, rng=rng
        )

        # Compute loss after training
        doc = docs[0]
        tokens = encode(doc, trained_vocab, trained_bos)
        n = min(cfg.block_size, len(tokens) - 1)
        kv_keys2: list[list[list[Scalar]]] = [[] for _ in range(cfg.n_layer)]
        kv_values2: list[list[list[Scalar]]] = [[] for _ in range(cfg.n_layer)]
        losses2: list[Scalar] = []
        trained_cfg = config.model_copy(update={"vocab_size": len(trained_vocab) + 1})
        for pos_id in range(n):
            token_id, target_id = tokens[pos_id], tokens[pos_id + 1]
            logits = gpt(
                token_id,
                pos_id,
                kv_keys2,
                kv_values2,
                trained_cfg,
                trained_sd,
            )
            probs = softmax(logits)
            loss_t = -probs[target_id].log()
            losses2.append(loss_t)
        final_loss_acc = Scalar(0.0)
        for l_t in losses2:
            final_loss_acc = final_loss_acc + l_t
        final_loss = final_loss_acc.data / n

        # Training should reduce loss
        assert final_loss < initial_loss

    def test_train_returns_correct_types(self) -> None:
        docs = ["ab", "ba"]
        config = GPTConfig(n_embd=4, n_head=2, block_size=4, n_layer=1)
        adam_config = AdamConfig()
        sd, vocab, bos = train(
            docs, config, adam_config, num_steps=2, rng=random.Random(42)
        )
        assert isinstance(sd, dict)
        assert isinstance(vocab, list)
        assert isinstance(bos, int)
        assert "wte" in sd


class TestSample:
    """Tests for sample generation."""

    def test_sample_produces_valid_tokens(self) -> None:
        docs = ["ab", "ba", "aa", "bb"]
        config = GPTConfig(n_embd=4, n_head=2, block_size=4, n_layer=1)
        adam_config = AdamConfig()
        sd, vocab, bos = train(
            docs, config, adam_config, num_steps=5, rng=random.Random(42)
        )
        sample_config = SampleConfig(temperature=0.5, max_tokens=4, num_samples=3)
        cfg = GPTConfig(
            n_embd=4,
            n_head=2,
            block_size=4,
            n_layer=1,
            vocab_size=len(vocab) + 1,
        )
        results = sample(sd, vocab, bos, cfg, sample_config, rng=random.Random(42))
        assert len(results) == 3
        for s in results:
            assert isinstance(s, str)
            # All chars should be in vocab
            for ch in s:
                assert ch in vocab

    def test_sample_respects_num_samples(self) -> None:
        docs = ["ab", "ba"]
        config = GPTConfig(n_embd=4, n_head=2, block_size=4, n_layer=1)
        adam_config = AdamConfig()
        sd, vocab, bos = train(
            docs, config, adam_config, num_steps=2, rng=random.Random(42)
        )
        cfg = GPTConfig(
            n_embd=4,
            n_head=2,
            block_size=4,
            n_layer=1,
            vocab_size=len(vocab) + 1,
        )
        sample_config = SampleConfig(temperature=1.0, max_tokens=4, num_samples=5)
        results = sample(sd, vocab, bos, cfg, sample_config, rng=random.Random(42))
        assert len(results) == 5


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestHelpers:
    """Tests for helper functions."""

    def test_matrix_shape(self) -> None:
        m = _matrix(3, 4, rng=random.Random(42))
        assert len(m) == 3
        assert len(m[0]) == 4

    def test_matrix_deterministic(self) -> None:
        m1 = _matrix(2, 3, rng=random.Random(42))
        m2 = _matrix(2, 3, rng=random.Random(42))
        for i in range(2):
            for j in range(3):
                assert m1[i][j].data == m2[i][j].data

    def test_get_params_count(self) -> None:
        config = GPTConfig(vocab_size=5, n_layer=1, n_embd=4, n_head=2, block_size=4)
        sd = init_state_dict(config, rng=random.Random(42))
        params = _get_params(sd)
        # wte: 5*4=20, wpe: 4*4=16, lm_head: 5*4=20
        # layer0 attn: 4*4*4=64 (wq+wk+wv+wo)
        # layer0 mlp: 16*4 + 4*16 = 64+64=128
        # Total: 20+16+20+64+128 = 248
        expected = (
            5 * 4  # wte
            + 4 * 4  # wpe
            + 5 * 4  # lm_head
            + 4 * (4 * 4)  # attn wq, wk, wv, wo
            + 16 * 4  # mlp_fc1
            + 4 * 16  # mlp_fc2
        )
        assert len(params) == expected


# ---------------------------------------------------------------------------
# Edge case / boundary condition tests
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_scalar_zero_mul(self) -> None:
        """Multiplying by zero should give zero with correct grads."""
        a = Scalar(5.0)
        b = Scalar(0.0)
        c = a * b
        c.backward()
        assert c.data == pytest.approx(0.0)
        assert a.grad == pytest.approx(0.0)  # grad = other.data = 0
        assert b.grad == pytest.approx(5.0)  # grad = self.data = 5

    def test_encode_empty_string(self) -> None:
        """Encoding empty string should give [BOS, BOS]."""
        vocab = ["a", "b"]
        bos = 2
        tokens = encode("", vocab, bos)
        assert tokens == [bos, bos]

    def test_decode_empty_token_list(self) -> None:
        vocab = ["a", "b"]
        bos = 2
        assert decode([], vocab, bos) == ""

    def test_build_vocab_empty_docs(self) -> None:
        vocab, bos = build_vocab([""])
        assert vocab == []
        assert bos == 0

    def test_softmax_two_elements(self) -> None:
        logits = [Scalar(0.0), Scalar(0.0)]
        probs = softmax(logits)
        assert probs[0].data == pytest.approx(0.5)
        assert probs[1].data == pytest.approx(0.5)

    def test_linear_single_element(self) -> None:
        x = [Scalar(3.0)]
        w = [[Scalar(2.0)]]
        result = linear(x, w)
        assert len(result) == 1
        assert result[0].data == pytest.approx(6.0)
