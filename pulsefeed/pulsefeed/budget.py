"""Per-tenant budget ledgers.

Budget here is not only a billing guard. It is an *input to the trigger*: as a
tenant's remaining budget falls, the bar for spending an LLM call rises, so the
day's last tokens are spent on the day's most important events rather than on
whatever happened to arrive at 4pm.

A slice of every budget is reserved for P0. A tenant that burns its whole
allowance on chatter still gets its incidents analysed.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from .models import Priority


@dataclass
class TenantPlan:
    """Static configuration for a tenant."""

    tenant_id: str
    daily_token_budget: int = 500_000
    daily_usd_budget: float = 5.0
    max_concurrent_llm_jobs: int = 4
    reserved_fraction_p0: float = 0.15
    allowed_models: tuple = ("primary", "fallback")
    retention_days: int = 30

    @classmethod
    def free(cls, tenant_id: str) -> "TenantPlan":
        return cls(
            tenant_id=tenant_id,
            daily_token_budget=5_000,
            daily_usd_budget=0.05,
            max_concurrent_llm_jobs=1,
            allowed_models=("fallback",),
        )

    @classmethod
    def pro(cls, tenant_id: str) -> "TenantPlan":
        return cls(tenant_id=tenant_id, daily_token_budget=500_000, daily_usd_budget=5.0)

    @classmethod
    def enterprise(cls, tenant_id: str, tokens: int = 20_000_000) -> "TenantPlan":
        return cls(
            tenant_id=tenant_id,
            daily_token_budget=tokens,
            daily_usd_budget=tokens / 1000.0 * 0.002,
            max_concurrent_llm_jobs=32,
            reserved_fraction_p0=0.25,
        )


@dataclass
class BudgetState:
    tokens_spent: int = 0
    usd_spent: float = 0.0
    day_start: float = field(default_factory=time.time)
    calls: int = 0
    denials: int = 0


class BudgetLedger:
    """Tracks spend per tenant and answers "can we afford this?".

    Deliberately not thread-safe by lock: the pipeline is single-event-loop
    asyncio, and charging happens at await-free points.
    """

    DAY_SECONDS = 86_400.0

    def __init__(self, default_plan: Optional[TenantPlan] = None) -> None:
        self._plans: Dict[str, TenantPlan] = {}
        self._state: Dict[str, BudgetState] = {}
        self._default_plan = default_plan

    def register(self, plan: TenantPlan) -> None:
        self._plans[plan.tenant_id] = plan
        self._state.setdefault(plan.tenant_id, BudgetState())

    def plan(self, tenant_id: str) -> TenantPlan:
        if tenant_id not in self._plans:
            base = self._default_plan or TenantPlan(tenant_id=tenant_id)
            self._plans[tenant_id] = TenantPlan(
                tenant_id=tenant_id,
                daily_token_budget=base.daily_token_budget,
                daily_usd_budget=base.daily_usd_budget,
                max_concurrent_llm_jobs=base.max_concurrent_llm_jobs,
                reserved_fraction_p0=base.reserved_fraction_p0,
                allowed_models=base.allowed_models,
                retention_days=base.retention_days,
            )
        return self._plans[tenant_id]

    def state(self, tenant_id: str, now: Optional[float] = None) -> BudgetState:
        now = now if now is not None else time.time()
        st = self._state.setdefault(tenant_id, BudgetState(day_start=now))
        if now - st.day_start >= self.DAY_SECONDS:
            st.tokens_spent = 0
            st.usd_spent = 0.0
            st.calls = 0
            st.denials = 0
            st.day_start = now
        return st

    # -- queries -----------------------------------------------------------

    def remaining_fraction(self, tenant_id: str, now: Optional[float] = None) -> float:
        """Fraction of the daily allowance still available, in [0, 1].

        Uses whichever of tokens/dollars is more constrained.
        """
        plan = self.plan(tenant_id)
        st = self.state(tenant_id, now)
        token_frac = (
            1.0 - st.tokens_spent / plan.daily_token_budget
            if plan.daily_token_budget > 0
            else 0.0
        )
        usd_frac = (
            1.0 - st.usd_spent / plan.daily_usd_budget
            if plan.daily_usd_budget > 0
            else 0.0
        )
        return max(0.0, min(1.0, min(token_frac, usd_frac)))

    def can_afford(
        self,
        tenant_id: str,
        estimated_tokens: int,
        estimated_usd: float = 0.0,
        priority: Priority = Priority.P2,
        now: Optional[float] = None,
    ) -> bool:
        """Whether this call fits, respecting the P0 reservation.

        Non-P0 work may only spend down to the reserved floor; P0 may spend the
        entire allowance. That is the whole point of the reservation: a noisy
        Tuesday must not be able to consume the capacity an incident will need.
        """
        plan = self.plan(tenant_id)
        st = self.state(tenant_id, now)

        if priority == Priority.P0:
            token_ceiling = float(plan.daily_token_budget)
            usd_ceiling = plan.daily_usd_budget
        else:
            token_ceiling = plan.daily_token_budget * (1.0 - plan.reserved_fraction_p0)
            usd_ceiling = plan.daily_usd_budget * (1.0 - plan.reserved_fraction_p0)

        ok = (
            st.tokens_spent + estimated_tokens <= token_ceiling
            and st.usd_spent + estimated_usd <= usd_ceiling
        )
        if not ok:
            st.denials += 1
        return ok

    # -- mutation ----------------------------------------------------------

    def charge(
        self,
        tenant_id: str,
        tokens: int,
        usd: float = 0.0,
        now: Optional[float] = None,
    ) -> None:
        st = self.state(tenant_id, now)
        st.tokens_spent += tokens
        st.usd_spent += usd
        st.calls += 1

    def snapshot(self, tenant_id: str, now: Optional[float] = None) -> Dict[str, float]:
        plan = self.plan(tenant_id)
        st = self.state(tenant_id, now)
        return {
            "tokens_spent": st.tokens_spent,
            "token_budget": plan.daily_token_budget,
            "usd_spent": round(st.usd_spent, 6),
            "usd_budget": plan.daily_usd_budget,
            "remaining_fraction": round(self.remaining_fraction(tenant_id, now), 4),
            "calls": st.calls,
            "denials": st.denials,
        }

    def reset(self) -> None:
        self._state.clear()
