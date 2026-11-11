"""Fair value (stage 3, step 6): what is a token worth right now?

    reference  F0b  strike K, window start, outcome per market (Gamma)
    inputs     F0c  1 s grid per token: book, Binance, σ, τ, K, outcome; backward joins only
    score      F1   one ruler for every candidate fair value
    models     F2-5 mid, microprice, Stoikov microprice, BTC digital (15m, ladder), Fed Σp = 1
    blend      F6   x_fair = w·x_model + (1 − w)·x_micro, w fitted on earlier days
    run        F7   `polymm fair`: the whole chain and the report
    walk            walk-forward one day in memory at a time (D-035); markouts use it too

Conventions: p is the YES (outcome 0) price, x = ln(p / (1 − p)) its log-odds, times are
UTC seconds on OUR receive clock. Every input joined onto second t was received at or
before t (asserted in `inputs`), so no candidate can see the future it is scored on.
"""
