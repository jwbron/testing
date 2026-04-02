# Plan: Refactor microgpt into modular library

## Summary

Refactor Karpathy's microgpt script into a modular `src/challenges/microgpt.py` with Pydantic config models, typed functions, and a comprehensive test suite. The original is a ~200-line monolithic script where all state is global — tokenizer logic is inline, model params are initialized at module scope, and training/inference are bare loops. We extract these into typed, reusable components. `Value` is renamed to `Scalar` (it represents a scalar node in a computation graph with autograd support). Pydantic `BaseModel` is used for all configuration objects (`GPTConfig`, `AdamConfig`, `SampleConfig`). `mypy` is added for static type checking.

**Risks / edge cases**: Tests must use tiny model configs (`n_embd=4, n_head=2, block_size=4`) to keep runtime under a few seconds. `Scalar` cannot be a Pydantic model due to operator overloads. Import-time side effects (the original downloads a file at top level) must be eliminated. `Scalar.__pow__` only supports constant exponents — this is by design and should be documented.

## Implementation

### Phase 1: Implement

Refactor the monolithic microgpt script into a modular library with Pydantic configs, full type annotations, and comprehensive tests.

**Tasks**:
1. **[task-1-1]** Update `pyproject.toml` — add `pydantic` to `[project.dependencies]`, add `mypy` to `[dependency-groups] dev`. Acceptance: `uv sync` succeeds with both new deps available.
2. **[task-1-2]** Create `src/challenges/microgpt.py` with Pydantic config models — `GPTConfig(n_layer, n_embd, block_size, n_head, vocab_size)` with computed `head_dim` property, `AdamConfig(learning_rate, beta1, beta2, eps)`, `SampleConfig(temperature, max_tokens, num_samples)`. These replace the scattered global constants in the original script. All fields should have sensible defaults matching the original values. Acceptance: models can be instantiated and validated.
3. **[task-1-3]** In `src/challenges/microgpt.py`, implement the `Scalar` class (renamed from `Value`) — a scalar autograd node supporting `+, *, **, /, -, neg, log, exp, relu` with forward computation and reverse-mode gradient accumulation via `backward()`. Uses topological sort to walk the computation graph. Must handle `Scalar op float` and `float op Scalar` via `__radd__`, `__rmul__`, etc. Acceptance: all operator combinations produce correct `.data` and `.grad` values.
4. **[task-1-4]** In `src/challenges/microgpt.py`, implement NN primitive functions — `linear(x: list[Scalar], w: list[list[Scalar]]) -> list[Scalar]` (matrix-vector multiply), `softmax(logits: list[Scalar]) -> list[Scalar]` (numerically stable via max subtraction), `rmsnorm(x: list[Scalar]) -> list[Scalar]` (root-mean-square normalization). These are currently top-level functions in the script but lack type annotations. Acceptance: each function is independently callable with correct output shapes and values.
5. **[task-1-5]** In `src/challenges/microgpt.py`, implement tokenizer functions — `build_vocab(docs: list[str]) -> tuple[list[str], int]` (extracts unique sorted chars, returns vocab + BOS ID; currently inline as `uchars = sorted(set(''.join(docs)))`), `encode(text: str, vocab: list[str], bos: int) -> list[int]` (wraps text in BOS tokens; currently inline as `[BOS] + [uchars.index(ch) for ch in doc] + [BOS]`), `decode(token_ids: list[int], vocab: list[str], bos: int) -> str` (inverse of encode; currently inline in the inference loop), `load_dataset(path: str, url: str | None = None) -> list[str]` (downloads if needed, reads lines; currently happens at module top level with side effects). Acceptance: `decode(encode(text)) == text` for valid inputs; `encode` raises `ValueError` on unknown chars.
6. **[task-1-6]** In `src/challenges/microgpt.py`, implement model initialization and forward pass — `init_state_dict(config: GPTConfig) -> dict[str, list[list[Scalar]]]` (creates all weight matrices: `wte`, `wpe`, `lm_head`, per-layer `attn_wq/wk/wv/wo` and `mlp_fc1/fc2`; currently scattered global code using a `matrix` lambda), `gpt(token_id: int, pos_id: int, keys: list[list[list[Scalar]]], values: list[list[list[Scalar]]], config: GPTConfig, state_dict: dict[str, list[list[Scalar]]]) -> list[Scalar]` (single-token forward pass through the transformer; currently a function but depends on global `state_dict` and `n_layer/n_head/head_dim`). Acceptance: forward pass returns `vocab_size` logits; deterministic for same inputs.
7. **[task-1-7]** In `src/challenges/microgpt.py`, implement training and inference functions — `adam_step(params: list[Scalar], m: list[float], v: list[float], step: int, config: AdamConfig) -> None` (one Adam optimizer step; currently 6 lines inside the training loop), `train(docs: list[str], config: GPTConfig, adam_config: AdamConfig, num_steps: int) -> tuple[dict[str, list[list[Scalar]]], list[str], int]` (full training loop returning trained state_dict, vocab, bos), `sample(state_dict: dict[str, list[list[Scalar]]], vocab: list[str], bos: int, config: GPTConfig, sample_config: SampleConfig) -> list[str]` (generates text samples; currently a bare loop using global state). Acceptance: `train` for 50 steps on tiny data shows decreasing loss; `sample` produces valid token sequences.
8. **[task-1-8]** Add type annotations throughout `src/challenges/microgpt.py` and configure mypy — ensure all public functions and class methods have full type annotations. Add a `[tool.mypy]` section to `pyproject.toml` with strict mode for the challenges package. Acceptance: `mypy src/challenges/microgpt.py` passes with no errors.
9. **[task-1-9]** Create `tests/test_microgpt.py` with granular `Scalar` autograd tests — `test_scalar_add` (Value(2)+Value(3): data=5, grads both 1.0), `test_scalar_mul` (Value(3)*Value(4): data=12, grads 4.0/3.0), `test_scalar_pow` (Value(2)**3: data=8, grad 12.0), `test_scalar_relu_positive` (passthrough, grad 1.0), `test_scalar_relu_negative` (data=0, grad 0.0), `test_scalar_log` (log(1)=0, grad 1.0), `test_scalar_exp` (exp(0)=1, grad 1.0), `test_scalar_composite_backward` (multi-op chain correctness), `test_scalar_division` (Value(6)/Value(3): data=2, correct grads). Acceptance: all 9 tests pass.
10. **[task-1-10]** In `tests/test_microgpt.py`, add NN primitive tests — `test_linear_identity` (identity matrix returns input), `test_softmax_uniform` (equal logits → uniform dist), `test_softmax_sums_to_one`, `test_softmax_numerical_stability` (large logits don't NaN), `test_rmsnorm_unit_scale` (output has ~unit RMS). Acceptance: all 5 tests pass.
11. **[task-1-11]** In `tests/test_microgpt.py`, add tokenizer tests — `test_build_vocab` (known input → expected vocab and BOS), `test_encode_decode_roundtrip` (decode(encode(text)) == text), `test_encode_unknown_char_raises` (ValueError on unknown char). Acceptance: all 3 tests pass.
12. **[task-1-12]** In `tests/test_microgpt.py`, add GPT model and end-to-end tests — `test_gpt_output_shape` (returns vocab_size logits), `test_gpt_deterministic` (same input = same output), `test_training_loss_decreases` (50 steps on tiny data), `test_sample_produces_valid_tokens`, `test_sample_respects_bos_termination`. All model tests use tiny config (`n_embd=4, n_head=2, block_size=4, n_layer=1`). Acceptance: all 5 tests pass.

```yaml
# yaml-tasks
pr:
  title: "Refactor microgpt into modular library with tests"
  description: |
    Refactors Karpathy's monolithic microgpt script into a modular Python library
    under src/challenges/microgpt.py with Pydantic config models, full type annotations
    (mypy-checked), and a comprehensive test suite covering autograd, NN primitives,
    tokenizer, and end-to-end training/inference.
  test_plan: |
    - Automated: pytest tests/test_microgpt.py (22 tests covering Scalar autograd, NN primitives, tokenizer, GPT model, and end-to-end)
    - Automated: mypy src/challenges/microgpt.py (strict type checking)
    - Automated: ruff check src/challenges/microgpt.py tests/test_microgpt.py (linting)
    - Manual: Review that Scalar class correctly implements all autograd operations from the original Value class
    - Manual: Verify Pydantic config defaults match original script constants
  manual_steps: |
    Pre-merge: Run uv sync to install pydantic and mypy dependencies
    Post-merge: None
phases:
  - id: 1
    name: Implement
    goal: "Refactor microgpt into modular library with Pydantic configs, type annotations, and comprehensive tests"
    tasks:
      - id: task-1-1
        description: "Update `pyproject.toml` — add `pydantic` to `[project.dependencies]`, add `mypy` to `[dependency-groups] dev`"
        acceptance: "uv sync succeeds with both new deps available"
        files:
          - pyproject.toml
      - id: task-1-2
        description: "Create `src/challenges/microgpt.py` with Pydantic config models — `GPTConfig(n_layer, n_embd, block_size, n_head, vocab_size)` with computed `head_dim` property, `AdamConfig(learning_rate, beta1, beta2, eps)`, `SampleConfig(temperature, max_tokens, num_samples)`. Defaults should match original script values."
        acceptance: "Models can be instantiated and validated with Pydantic"
        files:
          - src/challenges/microgpt.py
      - id: task-1-3
        description: "In `src/challenges/microgpt.py`, implement the `Scalar` class (renamed from `Value`) — a scalar autograd node supporting `+, *, **, /, -, neg, log, exp, relu` with forward computation and reverse-mode gradient accumulation via `backward()`. Uses topological sort. Must handle `Scalar op float` and `float op Scalar`."
        acceptance: "All operator combinations produce correct .data and .grad values"
        files:
          - src/challenges/microgpt.py
      - id: task-1-4
        description: "In `src/challenges/microgpt.py`, implement NN primitive functions — `linear(x, w)` (matrix-vector multiply), `softmax(logits)` (numerically stable via max subtraction), `rmsnorm(x)` (root-mean-square normalization). All with full type annotations."
        acceptance: "Each function is independently callable with correct output shapes and values"
        files:
          - src/challenges/microgpt.py
      - id: task-1-5
        description: "In `src/challenges/microgpt.py`, implement tokenizer functions — `build_vocab(docs)` (extract unique sorted chars + BOS), `encode(text, vocab, bos)` (tokenize with BOS wrapping), `decode(token_ids, vocab, bos)` (inverse of encode), `load_dataset(path, url)` (download if needed, read lines). No import-time side effects."
        acceptance: "decode(encode(text)) == text for valid inputs; encode raises ValueError on unknown chars"
        files:
          - src/challenges/microgpt.py
      - id: task-1-6
        description: "In `src/challenges/microgpt.py`, implement model init and forward pass — `init_state_dict(config)` (creates all weight matrices: wte, wpe, lm_head, per-layer attn_wq/wk/wv/wo and mlp_fc1/fc2), `gpt(token_id, pos_id, keys, values, config, state_dict)` (single-token transformer forward pass). Both take GPTConfig instead of relying on globals."
        acceptance: "Forward pass returns vocab_size logits; deterministic for same inputs"
        files:
          - src/challenges/microgpt.py
      - id: task-1-7
        description: "In `src/challenges/microgpt.py`, implement training and inference — `adam_step(params, m, v, step, config)` (one Adam update), `train(docs, config, adam_config, num_steps)` (full training loop returning state_dict, vocab, bos), `sample(state_dict, vocab, bos, config, sample_config)` (temperature-controlled text generation)."
        acceptance: "train for 50 steps shows decreasing loss; sample produces valid token sequences"
        files:
          - src/challenges/microgpt.py
      - id: task-1-8
        description: "Add type annotations throughout `src/challenges/microgpt.py` and configure mypy — add `[tool.mypy]` section to `pyproject.toml` with strict mode. Ensure all public functions and class methods have full type annotations."
        acceptance: "mypy src/challenges/microgpt.py passes with no errors"
        files:
          - src/challenges/microgpt.py
          - pyproject.toml
      - id: task-1-9
        description: "Create `tests/test_microgpt.py` with 9 granular Scalar autograd tests — test_scalar_add (data=5, grads 1.0), test_scalar_mul (data=12, grads 4.0/3.0), test_scalar_pow (data=8, grad 12.0), test_scalar_relu_positive (passthrough, grad 1.0), test_scalar_relu_negative (data=0, grad 0.0), test_scalar_log (log(1)=0, grad 1.0), test_scalar_exp (exp(0)=1, grad 1.0), test_scalar_composite_backward (multi-op chain), test_scalar_division (data=2, correct grads)."
        acceptance: "All 9 Scalar tests pass"
        files:
          - tests/test_microgpt.py
      - id: task-1-10
        description: "In `tests/test_microgpt.py`, add 5 NN primitive tests — test_linear_identity, test_softmax_uniform, test_softmax_sums_to_one, test_softmax_numerical_stability (large logits don't NaN), test_rmsnorm_unit_scale (output has ~unit RMS)."
        acceptance: "All 5 NN primitive tests pass"
        files:
          - tests/test_microgpt.py
      - id: task-1-11
        description: "In `tests/test_microgpt.py`, add 3 tokenizer tests — test_build_vocab (known input → expected vocab and BOS), test_encode_decode_roundtrip, test_encode_unknown_char_raises (ValueError)."
        acceptance: "All 3 tokenizer tests pass"
        files:
          - tests/test_microgpt.py
      - id: task-1-12
        description: "In `tests/test_microgpt.py`, add 5 GPT model and end-to-end tests — test_gpt_output_shape (vocab_size logits), test_gpt_deterministic (same input = same output), test_training_loss_decreases (50 steps tiny data), test_sample_produces_valid_tokens, test_sample_respects_bos_termination. All use tiny config (n_embd=4, n_head=2, block_size=4, n_layer=1)."
        acceptance: "All 5 GPT/e2e tests pass"
        files:
          - tests/test_microgpt.py
```