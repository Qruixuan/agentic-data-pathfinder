"""Adaptive Workload Model over a finite reduced design set."""

from .contracts import (
    AWM_CONFIG_SCHEMA_VERSION,
    AWM_CONFIG_SCHEMA_VERSION_V2,
    AWM_CONFIG_SCHEMA_VERSION_V2_1,
    AWM_CONFIG_SCHEMA_VERSIONS,
    AWMConfig,
    AWMConfigError,
    AssumptionConfig,
    ConfidenceConfig,
    load_awm_config,
)
from .dataset import (
    AWMDataset,
    DesignSample,
    TrialObservation,
    load_oracle_dataset,
)
from .evaluation import evaluate_awm, holdout_truth
from .model import (
    AWM_MODEL_KINDS,
    AdaptiveWorkloadModel,
    DesignBounds,
    EmpiricalBernsteinEstimate,
    Interval,
    PairedGainCertificate,
    PairedGainPowerAnalysis,
)

__all__ = [
    "AWM_CONFIG_SCHEMA_VERSION",
    "AWM_CONFIG_SCHEMA_VERSION_V2",
    "AWM_CONFIG_SCHEMA_VERSION_V2_1",
    "AWM_CONFIG_SCHEMA_VERSIONS",
    "AWM_MODEL_KINDS",
    "AWMConfig",
    "AWMConfigError",
    "AWMDataset",
    "AdaptiveWorkloadModel",
    "AssumptionConfig",
    "ConfidenceConfig",
    "DesignBounds",
    "DesignSample",
    "EmpiricalBernsteinEstimate",
    "Interval",
    "PairedGainCertificate",
    "PairedGainPowerAnalysis",
    "TrialObservation",
    "evaluate_awm",
    "holdout_truth",
    "load_awm_config",
    "load_oracle_dataset",
]
