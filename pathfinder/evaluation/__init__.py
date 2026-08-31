"""Offline evaluation of frozen Pathfinder workload results.

No service, executor, database, model, or credentials are needed. Evaluation
is descriptive: it does not issue an AWM certificate or an OED decision.
"""

from .distributed import EvaluationError, evaluate_distributed_pilot

__all__ = ["EvaluationError", "evaluate_distributed_pilot"]
