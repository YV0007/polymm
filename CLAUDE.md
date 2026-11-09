# Project: Market-Making Research on Polymarket

> Stable context: who I am, what I already understand, how we work.
> Read this fully before doing anything, then follow §0 to load the current state.

---

## 0. Sources of truth (read first, every chat)

Each fact lives in exactly one place (decision D-018). Don't copy between them; link.

| What | Where |
|---|---|
| **Decisions** (what we chose and why) | `docs/DECISIONS.md` in this repo, append-only. On a worktree/branch read the main copy: `/Users/macbook/Desktop/POLY MM/docs/DECISIONS.md` |
| **Data design** (datasets, schemas, verified quirks) | `docs/DATA_PLAN.md` |
| **Plans, today's steps, status, ops/cost** | Notion → *MM / Polymarket MM — Project* (page id `40fe45929c1e8104acb8fb8a9ade3a04`) → Plan → *Global plan* (`40fe45929c1e81b9983dd81a6aa613ff`), *Daily plan* (`40fe45929c1e81d883c4fa0b448339e3`) |
| **Data** | `data/` locally, `/home/polymm/polymm/data/` on the VPS (`ssh polymm`). Never in Notion or git. |
| **Reports** | `reports/`, `research/`; Notion gets at most a one-line summary + path |
| **Dashboard** (review surface) | https://claude.ai/artifact/Lp6FVttHUNMFGWziGZ5uBk, refreshed every morning; how to add a section: `docs/DASHBOARD.md` |

**At the start of a chat:** read `docs/DECISIONS.md` and today's section of the Notion Daily plan.
Other chats may be working in parallel (`git status`, recent commits): don't touch files they are changing.

**When you make a decision:** add a `D-NNN` entry to `docs/DECISIONS.md` in the same commit as the code.
**When you finish a plan step:** tick it in the Notion Daily plan.
**When a result is worth tracking:** add it to the dashboard in the same session (`docs/DASHBOARD.md`), keeping it to the stage's headline numbers and key charts.

---

## 1. Who I am and how I want to work

- **Yehor**, BSc student in Mathematical Foundations of Data Science (University of Vienna). Background in maths and data science, prior trader internship at a crypto exchange, options / GEX research experience.
- **Learning goal:** I want to improve my own Python through this project, by reading and running the code with its reasoning explained, not by doing separate exercises.

**Working style (D-005, important, follow it):**
1. **Claude builds the code end to end.** I run it and review it; I don't edit code.
2. Put `# Why:` comments on every non-obvious choice, and after each step **highlight the key decisions and explain why** they were made that way, so I learn from them.
3. Checks and audits run together with each build step (verify on live data, report numbers).
4. Before a large step, state the plan briefly; don't ask me to write skeletons.

**Tech plan:** Python for research, data collection, analysis and backtesting. A Rust live engine **only later**, if research shows an edge (Polymarket ships an official Rust SDK). Language choice for speed is irrelevant until an edge is proven.

---

## 2. The goal of the project

Find out, **with data and before risking money**, whether a market-making strategy on Polymarket has a positive edge after adverse selection. If it does, build toward a live quoting engine.

Not a goal right now: latency racing / sniping, or directional betting.

---

## 3. Polymarket structure (what I've learned)

- **Central Limit Order Book (CLOB)**, not an AMM. Prices emerge from users' limit orders.
- **Hybrid-decentralized:** off-chain matching by an operator, **on-chain settlement** on Polygon via the audited Exchange contract. Non-custodial: orders are **EIP-712 signed** messages; trading requests use HMAC-authenticated L2 API credentials.
- **Tokens:** each binary market has YES and NO outcome tokens. One pays $1 at resolution, the other $0.
  - **Split:** 1 collateral → 1 YES + 1 NO. **Merge:** 1 YES + 1 NO → 1 collateral.
  - Collateral has moved to **pUSD** (previously USDC.e). Clients are now **v2** (`py-clob-client-v2`, `@polymarket/clob-client-v2`, `polymarket_client_sdk_v2` for Rust). The legacy `py-clob-client` is retired. **Verify against current docs.**
- **Unified book:** a YES bid at p is economically a NO ask at 1−p. Matching can be normal (YES vs YES), **mint** (YES buyer + NO buyer → split) or **merge** (YES seller + NO seller).
  - Consequence: **net inventory = YES − NO**. Holding NO ≡ being "short YES".
- **Ticks:** normally 1¢, finer near the extremes on some markets.
- **Order types:** all orders are limit orders ("market order" = marketable limit). GTC, GTD (expires at a time — useful before events), FOK, FAK. **Post-only** orders are rejected if they would cross.
- **Sports markets:** marketable orders get a **1-second matching delay** (anti-sniping).
- **Matching:** price-time priority (FIFO at each price level).
- **Multi-outcome events:** a group of binary markets; mutually exclusive ones use **neg-risk**, so Σ p_j ≈ 1.
- **Resolution:** UMA optimistic oracle (propose → challenge window → dispute → token-holder vote).
- **Fees / incentives (verify current values per market):**
  - Taker fees exist in many market categories; they fund **maker rebates** paid daily.
  - **Liquidity rewards:** paid daily to resting orders near the mid. Score is roughly `S = ((v − s)/v)² · size` (s = distance from mid, v = max qualifying spread), with a min order size and two-sided quoting favoured. Most rewarded markets pay roughly $1–50/day, shared among makers.
- **Geo restrictions:** trading is blocked in some jurisdictions. **Check whether Austria is allowed before anything live.** Read-only data access works from anywhere.

### APIs (read-only needs no key)
| API | Base URL | Use |
|---|---|---|
| Gamma | `https://gamma-api.polymarket.com` | markets, events, metadata, `clobTokenIds` |
| CLOB | `https://clob.polymarket.com` | `/book?token_id=`, `/midpoint`, `/price`, `/prices-history`, trading |
| Data | `https://data-api.polymarket.com` | trades, positions, wallet activity |
| WebSocket | CLOB market channel | live book snapshots / deltas / trades |

- CLOB endpoints use **token IDs**, not Gamma market IDs.
- **There are no historical order book snapshots** from the API, so I must **record the WebSocket feed myself**.
- Docs: https://docs.polymarket.com (Polymarket US is a separate venue with separate docs).

---

## 4. Market-making concepts I understand

### Intuition
An MM is a shopkeeper selling immediacy: buy at the bid, sell at the ask, earn the spread when flow is balanced, keep inventory flat. The danger is **adverse selection**: trading against someone who knows more.

### Corrections I've internalised (don't re-explain unless I ask)
- **Direction:** if people **buy from me**, my ask was hit, so I am **short YES** (in practice: holding NO / having sold YES). To flatten after an up-jump I **buy back**, not sell.
- **The loss happens at the fill, not at the exit.** Selling at a stale 0.52 when the true value is 0.80 locks in the loss immediately; exiting just realises it. **A resting limit order is a free option given to the market.**
- Adverse selection is often **many fast traders sweeping all stale levels**, not one big trader. It also happens **continuously in small doses** (each fill is weakly informative).
- **Why spreads widen when risk rises:** (1) a wider spread filters out traders with small information edges; (2) it cushions the loss per bad fill; (3) the break-even spread rises with toxicity α.
- **Tick floor:** when the 1¢ tick is wider than the break-even spread (calm markets), MMs can't compete on price, so they compete on **queue position** and **rewards**. The **back of the queue is toxic**: it only gets filled by large sweeps.
- **Risk aversion γ:** how much variance costs me in P&L terms. Certainty equivalent ≈ E[X] − (γ/2)Var(X). It is a knob I choose.

### Key models / math
**Glosten–Milgrom (binary):** V ∈ {0,1}, belief p, fraction α of informed traders.
`ask = E[V|buy] = p(1+α) / [p(1+α) + (1−p)(1−α)]`; at p = 0.5, ask = (1+α)/2, so **half-spread = α/2**.
The spread is the fee noise traders pay to cover what informed traders take.

**Master P&L decomposition** (the backbone of the research). For fill i: side sᵢ (+1 buy, −1 sell), price Pᵢ, fair value mᵢ at the fill, mᵢᵗ fair value τ later, Vₜ final value:

$$
\text{P\&L} = \underbrace{\sum s_i(m_i - P_i)}_{①\ \text{spread}} + \underbrace{\sum s_i(m_i^{\tau} - m_i)}_{②\ \text{adverse selection}} + \underbrace{\sum s_i(V_T - m_i^{\tau})}_{③\ \text{inventory}} + \underbrace{R}_{④\ \text{rewards/rebates}} - F
$$

Most of the edge difference between MMs is in **term ②**.

**Markout:** `Mᵢ(τ) = sᵢ(mᵢᵗ − Pᵢ) = hᵢ − aᵢ(τ)`; edge per fill = `h̄ − ā(τ)`. Quote a side only if `h > E[a | z] + c`.

**Fair value toolkit:** mid → **microprice** `m = P_ask·Q_bid/(Q_bid+Q_ask) + P_bid·Q_ask/(Q_bid+Q_ask)` → work in **log-odds** x = ln(p/(1−p)) → precision-weighted combination of sources (Kalman filter) → structural constraints (Σp = 1, nested events, strike-ladder monotonicity) → external models (BTC threshold market = digital option `p = Φ(d₂)`).

**Inventory (Avellaneda–Stoikov):** fill intensity λ(δ) = A e^(−kδ);
reservation price `r = m − qγσ²(T−t)`; total spread `γσ²(T−t) + (2/γ)ln(1+γ/k)`.
Bounded-inventory version: Guéant–Lehalle–Fernandez-Tapia (skew linear in q).
**Binary twist:** σ_p ≈ p(1−p)σ_x; terminal variance q²p(1−p), so flatten before resolution.
**Portfolio:** `r_j = m_j − γ(T−t)(Σq)_j`; hedging across correlated markets lowers risk and allows **tighter quotes**.

**Jumps / cancel rule:** cancel if `λ(t)·E[(|J|−h)⁺]·Q > ρ` (expected jump loss rate > earning rate from staying).

**Queue value:** `V(x) = P_fill(x)·(h − E[a|fill,x]) + rewards`; P_fill falls and toxicity rises with queue position x, so there is a threshold beyond which queueing has negative value (Moallemi & Yuan).

**Market selection:** `πⱼ ≈ λⱼ(h̄ⱼ − āⱼ) + Rⱼ − γVarⱼ`; rank by **πⱼ / capitalⱼ**, since capital is locked until resolution.

### Speed (for later)
`L_total = L_feed + L_compute + L_network + L_venue`. On Polymarket, feed + network (ms) dominate, not code (µs). Measure each stage with timestamps before optimising anything.

### Return expectations (rough)
Small competent operator: good month +3–10%, normal +1–4%, bad −5 to −20% (jump events). The distribution is **asymmetric**. Many rewards farmers are net negative because they don't measure term ②. **Judge strategies by Sharpe and worst week, not average.**

---

## 5. Research plan and current status → Notion

The roadmap is the 8-stage **Global plan** in Notion (D-017): infrastructure → replay & data quality → shape of the market → fair value → adverse selection (⭐) → **Gate A** → queue & fill model → rewards & fees → simulator → **Gate B** → live pilot. Where we are and what's next today: **Daily plan**. Both are linked in §0; don't copy them here, they change daily.

What's built so far (`src/polymm`): recorder (Polymarket WS + Binance spot), REST poller with gap confirmation, universe watcher, Parquet/raw sinks, audit, backfill, replay; running 24/7 on the Hetzner VPS with a nightly pull home. Details in `docs/DECISIONS.md`.

---

## 6. Guidelines for you (Claude Code)

- **Verify against the current Polymarket docs** before relying on API details (endpoints, WebSocket message formats, fee/reward parameters, client versions). They change.
- Explain concepts in layers (intuition → detail → maths) when I ask "why".
- Be direct when my reasoning is wrong; I've had sign/direction confusions before (long vs short after fills), so check those.
- Research first, no live trading until the data shows edge.

---

## 7. Learning track: HFT quantitative analyst (standing instruction)

Yehor wants to come out of this project **proficient at the level of a working HFT quant analyst**, not just with a side project. The target skill set is this job description; the project is the vehicle:

1. Analyse multi-venue top-of-book / depth / trades / orders / fills / cancels → liquidity, price discovery, execution quality.
2. Latency-aware, event-driven backtests with explicit maker/taker execution, queue position, fill probability, fees, cancellations.
3. Spread capture, forward markouts, fees, adverse selection; how execution quality changes across venues / instruments / regimes.
4. Reusable Python tools and pipelines (columnar analysis + SQL) for research, attribution, monitoring.
5. Cross-venue price discovery, lead–lag, fair-value construction, spot–perp basis; data staleness, instrument design, settlement currency.
6. Live support: PnL / inventory / exposure analysis, post-trade review, investigating unexpected performance.
7. Liquidity-program economics: execution quality vs market share vs fees vs incentives; translating research into production.
8. Presenting findings with assumptions, limitations, risks, next steps.

**How to apply, in every reply where the work touches one of these:** say which skill it is, what a practitioner pays attention to and why, what changes the workflow or the model, and the common mistake. Short "note:" blocks are fine; do not lecture, teach at the point of use. When a number comes out, say how a desk would read it. Prefer the standard industry vocabulary (markout, toxicity, queue priority, signature plot, basis, realized spread, effective spread) and define it once.
