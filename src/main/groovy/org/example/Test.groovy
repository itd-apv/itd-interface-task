import de.itdesign.clarity.logging.CommonLogger
import de.itdesign.interfaces.odf.dto.OdfObject
import de.itdesign.interfaces.odf.utils.OdfDatabaseUtils
import groovy.sql.GroovyResultSet
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.transform.Field

import java.sql.Connection

@Field Sql sql
@Field CommonLogger cmnLog = new CommonLogger(this)
cmnLog.setFailJobOnError(true)

private List<GroovyRowResult> getOdfObjectRowResults(String odfObjectCode, Integer odfParentObjectDbId) {
    return sql.rows("""
            select
                obj.id                      as id
                , obj.code                  as code
                , nls.name                  as name
                , obj.api_alias             as api_alias
                , parent_obj.code           as parent_object_code
                , parent_obj.api_alias      as parent_object_api_alias
                , grandparent_obj.code      as grandparent_object_code
                , grandparent_obj.api_alias as grandparent_object_api_alias
                , case
                    when    (
                                obj.is_custom = 1
                                and obj.is_api_enabled = 1
                                and obj.template_extension is null
                                and obj.is_private_api = 0
                                and obj.is_abstract = 0
                            )
                    then 1
                    else 0
                  end                       as is_eligible
            from            odf_objects         obj
            inner join      cmn_captions_nls    nls             on  nls.pk_id = obj.id
                                                                and nls.table_name = 'ODF_OBJECTS'
                                                                and nls.language_code = 'en'
            left outer join odf_objects         parent_obj      on  parent_obj.code = obj.parent_object_code
            left outer join odf_objects         grandparent_obj on  grandparent_obj.code = parent_obj.parent_object_code
            where   obj.code = coalesce(?, obj.code)
            and     coalesce(parent_obj.id, -1) = coalesce(?, parent_obj.id, -1)
        """,[odfObjectCode,odfParentObjectDbId])
}

OdfObject getOdfObject(String odfObjectCode) {
    List<OdfObject> odfObjects = getOdfObjects(odfObjectCode, null)
    if (odfObjects.size() > 1) {
        throw new Exception("${odfObjects.size()} records are returned by the query to fetch the Object details with Object Code: ${odfObjectCode}, expected only one.")
    } else if (odfObjects.size() == 0) {
        throw new Exception("No record is returned by the query to fetch the Object details with Object Code: ${odfObjectCode}, expected one record.")
    }

    return odfObjects.first()
}

List<OdfObject> getOdfObjects(String odfObjectCode, Integer odfParentObjectDbId) {
    List<OdfObject> odfObjects = []

    if (odfObjectCode || odfParentObjectDbId) {
        List<GroovyRowResult> objectRowResults = getOdfObjectRowResults(odfObjectCode, odfParentObjectDbId)
        odfObjects = objectRowResults.collect { GroovyRowResult objectRowResult ->
            OdfObject odfObject = new OdfObject(
                    dbId: objectRowResult.id as Integer
                    , code: objectRowResult.code
                    , name: objectRowResult.name
                    , apiAlias: objectRowResult.api_alias
                    , parentCode: objectRowResult.parent_object_code
                    , parentApiAlias: objectRowResult.parent_object_api_alias
                    , grandparentCode: objectRowResult.grandparent_object_code
                    , grandparentApiAlias: objectRowResult.grandparent_object_api_alias
                    , isEligible: objectRowResult.is_eligible == 1
            )
            odfObject.subObjects = getOdfObjects(null, odfObject.dbId)
            odfObject
        }
    }

    odfObjects
}

private List<GroovyRowResult> getOdfObjectRowResult(String odfObjectCode, Integer odfParentObjectDbId) {
    cmnLog.info("getodfresults,code=${odfObjectCode},parent=${odfParentObjectDbId}")
    return sql.rows("""
            select * from dual
where   2 = coalesce(${odfParentObjectDbId}, 2, -1)
        """)
}

def runScript() {
    if (binding.variables.containsKey('sql')) {
        sql = binding.variables.get('sql') as Sql
    } else if (binding.variables.containsKey('connection')) {
        Connection conn = binding.variables.get('connection') as Connection
        sql = new Sql(conn)
    } else {
        throw new Exception('No SQL connection could be found, aborting.')
    }

    cmnLog.info "Execution Starts at ${new Date()}"
    boolean oldAutoCommit = sql.connection.autoCommit
    //OdfDatabaseUtils dbutils=new OdfDatabaseUtils(sql)
    // def res=getOdfObject("z_itd_intfc")
    List<GroovyRowResult> result = getOdfObjectRowResults(null,5040000)
    cmnLog.info("Result=${result}")
    return
    String odfObjectCode = null
    Integer odfParentObjectDbId = 5000000
    try {
        sql.connection.autoCommit = true
        String query = """
            select
                obj.id                      as id
                , obj.code                  as code
                , nls.name                  as name
                , obj.api_alias             as api_alias
                , parent_obj.code           as parent_object_code
                , parent_obj.api_alias      as parent_object_api_alias
                , grandparent_obj.code      as grandparent_object_code
                , grandparent_obj.api_alias as grandparent_object_api_alias
                , case
                    when    (
                                obj.is_custom = 1
                                and obj.is_api_enabled = 1
                                and obj.template_extension is null
                                and obj.is_private_api = 0
                                and obj.is_abstract = 0
                            )
                    then 1
                    else 0
                  end                       as is_eligible
            from            odf_objects         obj
            inner join      cmn_captions_nls    nls             on  nls.pk_id = obj.id
                                                                and nls.table_name = 'ODF_OBJECTS'
                                                                and nls.language_code = 'en'
            left outer join odf_objects         parent_obj      on  parent_obj.code = obj.parent_object_code
            left outer join odf_objects         grandparent_obj on  grandparent_obj.code = parent_obj.parent_object_code
            where   obj.code = coalesce(${odfObjectCode}, obj.code)
            and     coalesce(parent_obj.id, -1) = coalesce(${odfParentObjectDbId}, parent_obj.id, -1)
        """
        cmnLog.info("query=${query}")
        List<GroovyResultSet> resultSet = sql.rows(query)
        cmnLog.info("Result set=${resultSet}")
        cmnLog.info "Execution Finishes at ${new Date()}"
    } catch (Exception exception) {
        cmnLog.error exception.asString()
    } finally {
        sql.connection.autoCommit = oldAutoCommit
    }
}

runScript()

