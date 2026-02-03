from dvt.artifacts.resources.base import BaseResource, Docs, FileHash, GraphResource
from dvt.artifacts.resources.v1.analysis import Analysis
from dvt.artifacts.resources.v1.catalog import Catalog, CatalogWriteIntegrationConfig

# alias to latest resource definitions
from dvt.artifacts.resources.v1.components import (
    ColumnConfig,
    ColumnInfo,
    CompiledResource,
    Contract,
    DeferRelation,
    DependsOn,
    FreshnessThreshold,
    HasRelationMetadata,
    InjectedCTE,
    NodeVersion,
    ParsedResource,
    ParsedResourceMandatory,
    Quoting,
    RefArgs,
    Time,
)
from dvt.artifacts.resources.v1.config import (
    Hook,
    NodeAndTestConfig,
    NodeConfig,
    TestConfig,
    list_str,
    metas,
)
from dvt.artifacts.resources.v1.documentation import Documentation
from dvt.artifacts.resources.v1.exposure import (
    Exposure,
    ExposureConfig,
    ExposureType,
    MaturityType,
)
from dvt.artifacts.resources.v1.function import (
    Function,
    FunctionArgument,
    FunctionConfig,
    FunctionMandatory,
    FunctionReturns,
)
from dvt.artifacts.resources.v1.generic_test import GenericTest, TestMetadata
from dvt.artifacts.resources.v1.group import Group, GroupConfig
from dvt.artifacts.resources.v1.hook import HookNode
from dvt.artifacts.resources.v1.macro import Macro, MacroArgument, MacroDependsOn
from dvt.artifacts.resources.v1.metric import (
    ConstantPropertyInput,
    ConversionTypeParams,
    CumulativeTypeParams,
    Metric,
    MetricAggregationParams,
    MetricConfig,
    MetricInput,
    MetricInputMeasure,
    MetricTimeWindow,
    MetricTypeParams,
)
from dvt.artifacts.resources.v1.model import (
    CustomGranularity,
    Model,
    ModelConfig,
    ModelFreshness,
    TimeSpine,
)
from dvt.artifacts.resources.v1.owner import Owner
from dvt.artifacts.resources.v1.saved_query import (
    Export,
    ExportConfig,
    QueryParams,
    SavedQuery,
    SavedQueryConfig,
    SavedQueryMandatory,
)
from dvt.artifacts.resources.v1.seed import Seed, SeedConfig
from dvt.artifacts.resources.v1.semantic_layer_components import (
    FileSlice,
    MeasureAggregationParameters,
    NonAdditiveDimension,
    SourceFileMetadata,
    WhereFilter,
    WhereFilterIntersection,
)
from dvt.artifacts.resources.v1.semantic_model import (
    Defaults,
    Dimension,
    DimensionTypeParams,
    DimensionValidityParams,
    Entity,
    Measure,
    NodeRelation,
    SemanticLayerElementConfig,
    SemanticModel,
    SemanticModelConfig,
)
from dvt.artifacts.resources.v1.singular_test import SingularTest
from dvt.artifacts.resources.v1.snapshot import Snapshot, SnapshotConfig
from dvt.artifacts.resources.v1.source_definition import (
    ExternalPartition,
    ExternalTable,
    ParsedSourceMandatory,
    SourceConfig,
    SourceDefinition,
)
from dvt.artifacts.resources.v1.sql_operation import SqlOperation
from dvt.artifacts.resources.v1.unit_test_definition import (
    UnitTestConfig,
    UnitTestDefinition,
    UnitTestFormat,
    UnitTestInputFixture,
    UnitTestNodeVersions,
    UnitTestOutputFixture,
    UnitTestOverrides,
)
