import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1753660787307 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://fiap-postech-bigdataarchitecture/data-lake/"], "recurse": True}, transformation_ctx="AmazonS3_node1753660787307")

# Script generated for node Tratamento de colunas
Tratamentodecolunas_node1754156469879 = ApplyMapping.apply(frame=AmazonS3_node1753660787307, mappings=[("cod", "string", "codigo", "string"), ("asset", "string", "ativo", "string"), ("type", "string", "tipo", "string"), ("part", "string", "papel", "string"), ("date", "string", "data", "string")], transformation_ctx="Tratamentodecolunas_node1754156469879")

# Script generated for node Remoção de nulos
Remoodenulos_node1754156566633 = Tratamentodecolunas_node1754156469879.gs_null_rows()

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    codigo,
    ativo,
    tipo,
    MIN(CAST(REPLACE(papel, ',', '.') AS DOUBLE)) AS min_papel,
    MAX(CAST(REPLACE(papel, ',', '.') AS DOUBLE)) AS max_papel,
    AVG(CAST(REPLACE(papel, ',', '.') AS DOUBLE)) AS media_papel,
    MIN(to_date(data, 'dd/MM/yy')) AS min_data,
    MAX(to_date(data, 'dd/MM/yy')) AS max_data,
    DATEDIFF(MAX(to_date(data, 'dd/MM/yy')), '2024-08-01') AS diff_data
FROM myDataSource
GROUP BY
    codigo,
    ativo,
    tipo


'''
SQLQuery_node1754156891277 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Remoodenulos_node1754156566633}, transformation_ctx = "SQLQuery_node1754156891277")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1754156891277, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754154084560", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1754156891277.count() >= 1):
   SQLQuery_node1754156891277 = SQLQuery_node1754156891277.coalesce(1)
AmazonS3_node1754159099171 = glueContext.getSink(path="s3://fiap-postech-bigdataarchitecture/data-refined/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["max_data", "codigo"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1754159099171")
AmazonS3_node1754159099171.setCatalogInfo(catalogDatabase="refined_b3",catalogTableName="tb_b3_portifolio")
AmazonS3_node1754159099171.setFormat("glueparquet", compression="snappy")
AmazonS3_node1754159099171.writeFrame(SQLQuery_node1754156891277)
job.commit()