import time
import grpc
import uuid
import logging
from concurrent import futures

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Imports from generated gRPC code.
from com.rawlabs.protocol.das.v1.services import (
    health_service_pb2_grpc,
    health_service_pb2,
    tables_service_pb2_grpc,
    tables_service_pb2,
    functions_service_pb2_grpc,
    functions_service_pb2,
    registration_service_pb2_grpc,
    registration_service_pb2,
)
from com.rawlabs.protocol.das.v1.tables import tables_pb2
from com.rawlabs.protocol.das.v1.common import das_pb2, environment_pb2
from com.rawlabs.protocol.das.v1.types import types_pb2, values_pb2
from com.rawlabs.protocol.das.v1.query import query_pb2, quals_pb2, operators_pb2


# Helper functions for building Values

def make_int_value(x):
    return values_pb2.Value(int=values_pb2.ValueInt(v=x))

def make_string_value(s):
    return values_pb2.Value(string=values_pb2.ValueString(v=s))


# ------------------------------------------------------------------------------
# MockTable
# ------------------------------------------------------------------------------
class MockTable:
    """
    Represents a mock table with a specified number of rows.
    Demonstrates returning definitions, sorting, path keys, estimates, etc.
    """

    def __init__(self, nrows, table_name="mock_table"):
        self.nrows = nrows  # how many rows this table has
        self.table_name = table_name

    def get_definitions(self):
        return tables_pb2.TableDefinition(
            table_id=tables_pb2.TableId(name=self.table_name),
            description=f"A mock table with {self.nrows} rows.",
            columns=[
                tables_pb2.ColumnDefinition(
                    name="id",
                    description="Primary key",
                    type=types_pb2.Type(int=types_pb2.IntType(nullable=False))
                ),
                tables_pb2.ColumnDefinition(
                    name="name",
                    description="Name field",
                    type=types_pb2.Type(string=types_pb2.StringType(nullable=False))
                ),
            ],
            startup_cost=1
        )

    def get_sort_orders(self, sort_keys):
        """
        Return a list of recognized sort keys, or empty if none.
        For now, we say we don't support ordering on our side.
        """
        return []

    def get_path_keys(self):
        """
        Return path-keys used for some optimizer logic. 
        For now, we say we don't support read by key on our side.
        """
        return []

    def get_estimate(self, quals, columns):
        """
        Return a static estimate of rows & bytes
        """
        rows = 100
        bytes_ = 200
        return (rows, bytes_)

    def explain(self, query):
        """
        Return a mock plan explanation.
        """
        return [
            f"Text shown when EXPLAIN SELECT is called on table {self.table_name}",
            "Another line of text"
        ]

    def execute(self, query, context=None):
        """
        Streaming RPC: returns a "closeable" generator of Rows messages.
        We:
        - Generate rows from 1..N (self.nrows).
        - Apply query filters (simple integer filter on "id").
        - Return only requested columns.
        - Respect query.limit if set.
        - Stream in small batches.
        - If the generator is closed (e.g., via .close()), we do final cleanup in 'finally'.
        """
        logger.debug("Entering MockTable.execute for table='%s'", self.table_name)
        
        max_rows = query.limit if query.HasField("limit") else self.nrows
        requested_cols = query.columns if query.columns else ["id", "name"]
        batch_size = 5
        current_batch = []

        try:
            for i in range(1, self.nrows + 1):
                # If the gRPC context is canceled, we can break early
                if context and not context.is_active():
                    logger.warning("Client canceled streaming for table='%s'. Stopping early.", self.table_name)
                    break

                if i > max_rows:
                    break

                # Apply qualifiers
                if not self._row_matches_quals(i, query.quals):
                    continue

                # Build the row's columns
                row_cols = []
                for col_name in requested_cols:
                    if col_name == "id":
                        row_cols.append(
                            tables_pb2.Column(
                                name="id",
                                data=make_int_value(i)
                            )
                        )
                    elif col_name == "name":
                        row_cols.append(
                            tables_pb2.Column(
                                name="name",
                                data=make_string_value(f"Mock row #{i}")
                            )
                        )
                    else:
                        row_cols.append(
                            tables_pb2.Column(
                                name=col_name,
                                data=make_string_value(f"Value for {col_name} @ row {i}")
                            )
                        )

                current_batch.append(tables_pb2.Row(columns=row_cols))

                # If we've reached a full batch, yield it
                if len(current_batch) >= batch_size:
                    yield tables_pb2.Rows(rows=current_batch)
                    current_batch = []

            # Yield leftover rows
            if current_batch and (not context or context.is_active()):
                yield tables_pb2.Rows(rows=current_batch)

        finally:
            # This block runs if the generator is exhausted or forcibly closed.
            logger.info("Generator for table='%s' is closing. Cleanup logic can go here.", self.table_name)
            # If you had resources to free, you'd do that here.

    def _row_matches_quals(self, row_id, quals):
        """
        Simplified pattern match on id-based filters:
          - SimpleQual with name="id" and an operator + int value.
          - Only EQUALS, NOT_EQUALS, GREATER_THAN, etc. are handled.
        """
        for q in quals:
            if q.name == "id" and q.HasField("simple_qual"):
                sq = q.simple_qual
                if sq.value.HasField("int"):
                    target_val = sq.value.int.v
                    op = sq.operator
                    if op == operators_pb2.EQUALS:
                        if row_id != target_val:
                            return False
                    elif op == operators_pb2.NOT_EQUALS:
                        if row_id == target_val:
                            return False
                    elif op == operators_pb2.GREATER_THAN:
                        if row_id <= target_val:
                            return False
                    elif op == operators_pb2.GREATER_THAN_OR_EQUAL:
                        if row_id < target_val:
                            return False
                    elif op == operators_pb2.LESS_THAN:
                        if row_id >= target_val:
                            return False
                    elif op == operators_pb2.LESS_THAN_OR_EQUAL:
                        if row_id > target_val:
                            return False
        return True

    def get_unique_column(self):
        return "id"

    def insert(self, row):
        """
        Mock insert. If returning None, we consider it unsupported.
        """
        return None

    def get_bulk_insert_size(self):
        return 1

    def bulk_insert(self, rows):
        """
        Mock bulk insert. If returning None, we consider it unsupported.
        """
        return None

    def update(self, row_id, row):
        """
        Mock update. If returning None, we consider it unsupported.
        """
        return None

    def delete(self, row_id):
        """
        Mock delete. If returning None, we consider it unsupported.
        """
        return None


# ------------------------------------------------------------------------------
# The global dictionary to track registered DAS instances: das_id -> MockDAS
# ------------------------------------------------------------------------------
active_dases = {}


# ------------------------------------------------------------------------------
# MockDAS
# ------------------------------------------------------------------------------
class MockDAS:
    """
    Represents a mock "DAS instance" that can contain multiple tables.
    We create two tables: a small one with 10 rows, and a large with 1000 rows.
    Each "instance" of this class could connect to a different instance of a remote server.
    When the instance is created, "options" are passed, which can be read to obtain the properties necessary to connect to that remote server.
    """

    def __init__(self, das_id, options):
        # If something fails here, we can throw an exception
        # e.g. we could parse "options" to obtain connection information and establish it.
        self.das_id = das_id
        self.options = options or {}

        # LOG the 'options' field
        logger.info("Creating MockDAS for das_id=%s with options=%s", das_id, self.options)

        self.small_table = MockTable(nrows=10, table_name="small_table")
        self.large_table = MockTable(nrows=100000000, table_name="large_table")

    def get_definitions(self):
        return [
            self.small_table.get_definitions(),
            self.large_table.get_definitions(),
        ]

    def get_table(self, table_id):
        if table_id.name == "small_table":
            return self.small_table
        elif table_id.name == "large_table":
            return self.large_table
        else:
            raise ValueError(f"Unknown table: {table_id.name}")

    def close(self):
        pass


# ------------------------------------------------------------------------------
# Helper to look up the DAS or throw NOT_FOUND, which triggers clients to re-register
# ------------------------------------------------------------------------------
def get_das_or_error(das_id: das_pb2.DASId, context):
    das_key = das_id.id
    if das_key not in active_dases:
        context.abort(grpc.StatusCode.NOT_FOUND, f"DAS not found: {das_key}")
    return active_dases[das_key]


# ------------------------------------------------------------------------------
# 1. HealthCheckService
# ------------------------------------------------------------------------------
class HealthCheckServiceServicer(health_service_pb2_grpc.HealthCheckServiceServicer):
    def Check(self, request, context):
        logger.debug("HealthCheckServiceServicer.Check called")
        return health_service_pb2.HealthCheckResponse(
            status=health_service_pb2.HealthCheckResponse.ServingStatus.SERVING,
            description="Mock server is healthy."
        )


# ------------------------------------------------------------------------------
# 2. RegistrationService
# ------------------------------------------------------------------------------
class RegistrationServiceServicer(registration_service_pb2_grpc.RegistrationServiceServicer):
    """
    rpc Register(RegisterRequest) returns (RegisterResponse);
    rpc Unregister(DASId) returns (UnregisterResponse);
    """

    def Register(self, request, context):
        """
        Only supports 'definition.type == "mock"'.
        If building MockDAS fails, we return an error in RegisterResponse.
        Otherwise, we set the 'id' field in the response.
        """
        logger.debug("RegistrationServiceServicer.Register called with definition=%s", request.definition)
        definition = request.definition
        if definition.type != "mock":
            # Return error in the RegisterResponse.
            logger.warning("Unsupported DAS type: %s", definition.type)
            return registration_service_pb2.RegisterResponse(
                error=f"Unsupported DAS type: {definition.type}"
            )

        # If request has an ID, use that. Otherwise generate one.
        if request.HasField("id") and request.id.id:
            das_key = request.id.id
            logger.info("Register request re-using existing ID: %s", das_key)
        else:
            das_key = str(uuid.uuid4())
            logger.info("Generated new DAS ID: %s", das_key)

        # If already active, do nothing
        if das_key in active_dases:
            logger.info("DAS %s is already registered. Returning existing ID.", das_key)
            return registration_service_pb2.RegisterResponse(
                id=das_pb2.DASId(id=das_key)
            )

        # Otherwise, create a new MockDAS
        try:
            mock_das = MockDAS(das_id=das_key, options=definition.options)
        except Exception as e:
            # Return error string
            logger.exception("Failed to build MockDAS: %s", e)
            return registration_service_pb2.RegisterResponse(error=str(e))

        active_dases[das_key] = mock_das
        logger.info("DAS %s registered successfully.", das_key)
        return registration_service_pb2.RegisterResponse(
            id=das_pb2.DASId(id=das_key)
        )

    def Unregister(self, request, context):
        das_key = request.id
        logger.debug("Unregister called for DAS %s", das_key)
        if das_key not in active_dases:
            logger.warning("Tried to unregister non-existing DAS: %s", das_key)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"DAS not found: {das_key}")

        instance = active_dases.pop(das_key)
        instance.close()
        logger.info("DAS %s unregistered successfully.", das_key)
        return registration_service_pb2.UnregisterResponse()


# ------------------------------------------------------------------------------
# 3. TablesService
# ------------------------------------------------------------------------------
class TablesServiceServicer(tables_service_pb2_grpc.TablesServiceServicer):
    """
    Implementation of all table-related RPC calls.
    """

    def GetTableDefinitions(self, request, context):
        logger.debug("GetTableDefinitions called for das_id=%s", request.das_id.id)
        das_instance = get_das_or_error(request.das_id, context)
        defs = das_instance.get_definitions()
        return tables_service_pb2.GetTableDefinitionsResponse(definitions=defs)

    def GetTableSortOrders(self, request, context):
        logger.debug("GetTableSortOrders called for table_id=%s", request.table_id.name)
        das_instance = get_das_or_error(request.das_id, context)
        try:
            table = das_instance.get_table(request.table_id)
        except ValueError as e:
            logger.warning("Invalid table_id for sort orders: %s", e)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        sort_orders = table.get_sort_orders(request.sort_keys)
        return tables_service_pb2.GetTableSortOrdersResponse(sort_keys=sort_orders)

    def GetTablePathKeys(self, request, context):
        logger.debug("GetTablePathKeys called for table_id=%s", request.table_id.name)
        das_instance = get_das_or_error(request.das_id, context)
        try:
            table = das_instance.get_table(request.table_id)
        except ValueError as e:
            logger.warning("Invalid table_id for path keys: %s", e)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        path_keys = table.get_path_keys()
        return tables_service_pb2.GetTablePathKeysResponse(path_keys=path_keys)

    def GetTableEstimate(self, request, context):
        logger.debug("GetTableEstimate called for table_id=%s", request.table_id.name)
        das_instance = get_das_or_error(request.das_id, context)
        try:
            table = das_instance.get_table(request.table_id)
        except ValueError as e:
            logger.warning("Invalid table_id for estimate: %s", e)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        rows, bytes_ = table.get_estimate(request.quals, request.columns)
        return tables_service_pb2.GetTableEstimateResponse(rows=rows, bytes=bytes_)

    def ExplainTable(self, request, context):
        logger.debug("ExplainTable called for table_id=%s", request.table_id.name)
        das_instance = get_das_or_error(request.das_id, context)
        try:
            table = das_instance.get_table(request.table_id)
        except ValueError as e:
            logger.warning("Invalid table_id for explain: %s", e)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        stmts = table.explain(request.query)
        return tables_service_pb2.ExplainTableResponse(stmts=stmts)

    def ExecuteTable(self, request, context):
        logger.debug("ExecuteTable called for table_id=%s", request.table_id.name)
        das_instance = get_das_or_error(request.das_id, context)
        try:
            table = das_instance.get_table(request.table_id)
        except ValueError as e:
            logger.warning("Invalid table_id for execute: %s", e)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

        # Get the closeable generator
        gen = table.execute(request.query, context)
        
        try:
            for batch in gen:
                if not context.is_active():
                    logger.warning("ExecuteTable streaming canceled for table_id=%s. Closing generator.", request.table_id.name)
                    gen.close()  # forcibly close
                    break
                yield batch
        finally:
            logger.debug("Leaving ExecuteTable for table_id=%s (generator closed or done).", request.table_id.name)

    def GetTableUniqueColumn(self, request, context):
        logger.debug("GetTableUniqueColumn called for table_id=%s", request.table_id.name)
        das_instance = get_das_or_error(request.das_id, context)
        try:
            table = das_instance.get_table(request.table_id)
        except ValueError as e:
            logger.warning("Invalid table_id for unique column: %s", e)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        col = table.get_unique_column()
        return tables_service_pb2.GetTableUniqueColumnResponse(column=col)

    def InsertTable(self, request, context):
        logger.debug("InsertTable called for table_id=%s", request.table_id.name)
        das_instance = get_das_or_error(request.das_id, context)
        try:
            table = das_instance.get_table(request.table_id)
        except ValueError as e:
            logger.warning("Invalid table_id for insert: %s", e)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

        result = table.insert(request.row)
        if result is None:
            logger.info("Insert not supported for table_id=%s", request.table_id.name)
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "Insert not supported.")

        return tables_service_pb2.InsertTableResponse(row=request.row)

    def GetBulkInsertTableSize(self, request, context):
        logger.debug("GetBulkInsertTableSize called for table_id=%s", request.table_id.name)
        das_instance = get_das_or_error(request.das_id, context)
        try:
            table = das_instance.get_table(request.table_id)
        except ValueError as e:
            logger.warning("Invalid table_id for GetBulkInsertTableSize: %s", e)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

        size_ = table.get_bulk_insert_size()
        return tables_service_pb2.GetBulkInsertTableSizeResponse(size=size_)

    def BulkInsertTable(self, request, context):
        logger.debug("BulkInsertTable called for table_id=%s", request.table_id.name)
        das_instance = get_das_or_error(request.das_id, context)
        try:
            table = das_instance.get_table(request.table_id)
        except ValueError as e:
            logger.warning("Invalid table_id for BulkInsert: %s", e)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

        result = table.bulk_insert(request.rows)
        if result is None:
            logger.info("Bulk insert not supported for table_id=%s", request.table_id.name)
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "Bulk insert not supported.")

        return tables_service_pb2.BulkInsertTableResponse(rows=request.rows)

    def UpdateTable(self, request, context):
        logger.debug("UpdateTable called for table_id=%s", request.table_id.name)
        das_instance = get_das_or_error(request.das_id, context)
        try:
            table = das_instance.get_table(request.table_id)
        except ValueError as e:
            logger.warning("Invalid table_id for update: %s", e)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

        result = table.update(request.row_id, request.new_row)
        if result is None:
            logger.info("Update not supported for table_id=%s", request.table_id.name)
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "Update not supported.")

        return tables_service_pb2.UpdateTableResponse(row=request.new_row)

    def DeleteTable(self, request, context):
        logger.debug("DeleteTable called for table_id=%s", request.table_id.name)
        das_instance = get_das_or_error(request.das_id, context)
        try:
            table = das_instance.get_table(request.table_id)
        except ValueError as e:
            logger.warning("Invalid table_id for delete: %s", e)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

        result = table.delete(request.row_id)
        if result is None:
            logger.info("Delete not supported for table_id=%s", request.table_id.name)
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "Delete not supported.")

        return tables_service_pb2.DeleteTableResponse()


# ------------------------------------------------------------------------------
# 4. The server entry point
# ------------------------------------------------------------------------------
def serve(port=50051):
    """
    Creates a gRPC server, registers all servicers, and starts listening.
    """
    logger.info("Creating gRPC server on port %s", port)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # HealthCheckService
    health_service_pb2_grpc.add_HealthCheckServiceServicer_to_server(
        HealthCheckServiceServicer(), server
    )

    # TablesService
    tables_service_pb2_grpc.add_TablesServiceServicer_to_server(
        TablesServiceServicer(), server
    )

    # RegistrationService
    registration_service_pb2_grpc.add_RegistrationServiceServicer_to_server(
        RegistrationServiceServicer(), server
    )

    # (FunctionsService not shown, but you could add here if needed)
    # functions_service_pb2_grpc.add_FunctionsServiceServicer_to_server(..., server)

    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"[MOCK] gRPC server started on port {port}.")
    print("[MOCK] Services: HealthCheckService, TablesService, RegistrationService.")
    logger.info("gRPC server started on port %s", port)
    logger.info("Services: HealthCheckService, TablesService, RegistrationService.")
    try:
        while True:
            time.sleep(24 * 60 * 60)
    except KeyboardInterrupt:
        logger.info("Shutting down gRPC server...")
        print("[MOCK] Shutting down server...")
        server.stop(0)