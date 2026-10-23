from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray


def row_jdbc_sink(sql: str,
                  type_info: RowTypeInfo,
                  jdbc_connection_options: JdbcConnectionOptions,
                  jdbc_execution_options: JdbcExecutionOptions = None) -> JdbcSink:
    """Drop-in replacement for PyFlink's JdbcSink.sink().

    PyFlink 1.20 looks up createRowJdbcStatementBuilder on JdbcOutputFormat, but
    flink-connector-jdbc 3.2+ moved it to RowJdbcOutputFormat.
    """
    gateway = get_gateway()
    j_type_util = gateway.jvm.org.apache.flink.connector.jdbc.utils.JdbcTypeUtil
    sql_types = [j_type_util.typeInformationToSqlType(field_type.get_java_type_info())
                 for field_type in type_info.get_field_types()]
    j_sql_types = to_jarray(gateway.jvm.int, sql_types)

    output_format_clz = gateway.jvm.Class.forName(
        'org.apache.flink.connector.jdbc.internal.RowJdbcOutputFormat', False,
        gateway.jvm.Thread.currentThread().getContextClassLoader())
    j_int_array_type = to_jarray(gateway.jvm.int, []).getClass()
    j_builder_method = output_format_clz.getDeclaredMethod(
        'createRowJdbcStatementBuilder', to_jarray(gateway.jvm.Class, [j_int_array_type]))
    j_builder_method.setAccessible(True)
    j_statement_builder = j_builder_method.invoke(None, to_jarray(gateway.jvm.Object, [j_sql_types]))

    if jdbc_execution_options is None:
        jdbc_execution_options = JdbcExecutionOptions.defaults()
    j_jdbc_sink = gateway.jvm.org.apache.flink.connector.jdbc.JdbcSink.sink(
        sql, j_statement_builder,
        jdbc_execution_options._j_jdbc_execution_options,
        jdbc_connection_options._j_jdbc_connection_options)
    return JdbcSink(j_jdbc_sink=j_jdbc_sink)
