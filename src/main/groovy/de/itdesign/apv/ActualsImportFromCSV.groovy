package de.itdesign.apv

import com.niku.union.security.SecurityIdentifier
import de.itdesign.interfaces.actuals.dto.Investment
import de.itdesign.interfaces.cmn.dto.enums.InterfaceOperationTypeEnum
import de.itdesign.interfaces.cmn.dto.enums.InterfaceSourceTypeEnum
import de.itdesign.interfaces.cmn.dto.enums.InterfaceStatusEnum
import de.itdesign.interfaces.cmn.dto.enums.InterfaceTypeEnum
import de.itdesign.interfaces.cmn.service.LookupService
import de.itdesign.interfaces.io.dto.enums.FileSortingTypeEnum
import de.itdesign.interfaces.odf.dto.OdfAttribute
import de.itdesign.interfaces.odf.dto.StgToTgt
import de.itdesign.interfaces.shared.InterfaceProcessor
import de.itdesign.interfaces.shared.actuals.ActualsInterfaceProcessor
import de.itdesign.interfaces.shared.dto.enums.DBVendor
import de.itdesign.interfaces.utils.rest.CommonRestUtils
import de.itdesign.interfaces.utils.rest.dto.EntityRestResponse
import de.itdesign.interfaces.utils.rest.dto.RestResponse
import de.itdesign.interfaces.validation.FieldValidation
import de.itdesign.interfaces.validation.RowValidation
import de.itdesign.interfaces.validation.StagingValidation
import de.itdesign.interfaces.validation.domain.Validation
import de.itdesign.interfaces.validation.utils.OutputFormatter
import de.itdesign.interfaces.validation.utils.ValidationRule
import groovy.sql.Sql

import java.sql.Connection
import java.text.DecimalFormat
import java.text.NumberFormat
import java.text.ParseException

class ActualsImportFromCSV {
    Sql sql
    Object log
    ActualsInterfaceProcessor actualsInterfaceProcessor

    ActualsImportFromCSV(Connection connection, Object log, CommonRestUtils restUtils, String configInstanceCode) {
        this.sql = new Sql(connection)
        this.log = log
        this.actualsInterfaceProcessor = new ActualsInterfaceProcessor(connection, restUtils, log, InterfaceTypeEnum.ACTUALS, InterfaceSourceTypeEnum.CSV, configInstanceCode)
    }

    List<Integer> importSrcDataIntoStg() {
        return actualsInterfaceProcessor.importSrcDataIntoStg(FileSortingTypeEnum.FILENAME_TIMESTAMP, false)
    }

    InterfaceProcessor validateAndTransform() {
        RowValidation rowValidation = initRowValidation()
        StagingValidation stagingValidation = new StagingValidation(actualsInterfaceProcessor.ctx, log, true)
        RestResponse validationRestResponse = stagingValidation.validate(rowValidation, InterfaceStatusEnum.NEW, InterfaceStatusEnum.VALIDATED, InterfaceStatusEnum.FAILED, InterfaceStatusEnum.WARNING, InterfaceStatusEnum.VALIDATED)

        validateDeltaUnitsAndCost()

        return actualsInterfaceProcessor
    }

    void writeToTarget() {
        actualsInterfaceProcessor.actualsDatabaseUtils.deletePendingTransactions()
        processActuals()
        if (actualsInterfaceProcessor.ctx.configInstance.postTransactions) {
            actualsInterfaceProcessor.createStagingRecordsForRemainingCost()
            processActuals()
        }
    }

    void processActuals() {
        processActuals(InterfaceStatusEnum.VALIDATED)
        processActuals(InterfaceStatusEnum.WARNING)

        if (actualsInterfaceProcessor.ctx.configInstance.postTransactions) {
            actualsInterfaceProcessor.actualsDatabaseUtils.updateErrorMessageForErroneousTransactions(actualsInterfaceProcessor.ctx.masterInstanceDbId)
            // actualsInterfaceProcessor.actualsDatabaseUtils.deletePendingTransactions()
        }

        Integer numberOfRecordsProcessed = actualsInterfaceProcessor.ctx.stagingObjectService.markStagingRecordsAsProcessed()
    }

    def processActuals(InterfaceStatusEnum status) {
        processActualsWithZeroUnits(status)
        processActualsWithNonZeroUnits(status)
    }

    def processActualsWithZeroUnits(InterfaceStatusEnum status) {
        EntityRestResponse restResponse = actualsInterfaceProcessor.createTransactionsWithZeroUnits(status)
        log?.info "Created ${status} Transactions with Zero Units: ${restResponse}"

        actualsInterfaceProcessor.ctx.stagingObjectService.setMasterObjectMessage("Statistics from processing ${status} Transactions with Zero Units: [Created transactions: ${restResponse}]")
    }

    def processActualsWithNonZeroUnits(InterfaceStatusEnum status) {
        EntityRestResponse restResponse = actualsInterfaceProcessor.createTransactionsWithNonZeroUnits(status)
        log?.info "Created ${status} Transactions with Non-Zero Units: ${restResponse}"

        actualsInterfaceProcessor.ctx.stagingObjectService.setMasterObjectMessage("Statistics from processing ${status} Transactions with Non-Zero Units: [Created transactions: ${restResponse}]")
    }

    void validateDeltaUnitsAndCost() {
        SecurityIdentifier securityIdentifier = actualsInterfaceProcessor.ctx.restUtils.securityIdentifier
        Map<String, OdfAttribute> subobjectAttrApiToAttrMap = actualsInterfaceProcessor.ctx.stagingObjectService.subobjectAttrApiToAttrMap
        Map<String, StgToTgt> stgAttrApiToStgToTgtInstanceMap = actualsInterfaceProcessor.ctx.stgAttrApiToStgToTgtInstanceMap
        LookupService lookupService = new LookupService(sql)

        RowValidation rowValidation = new RowValidation(
                subobjectAttrApiToAttrMap,
                stgAttrApiToStgToTgtInstanceMap,
                [
                        new FieldValidation(
                                ["_internalId"]
                                , "z_delta_units_cl"
                                , []
                                , OutputFormatter.&asDynamicLookupValue.curry(securityIdentifier, lookupService, "Z_ITD_DELTA_UNITS_COST", "delta_units", [:], ["stg_id"])
                        ),
                        new FieldValidation(
                                ["_internalId"]
                                , "z_delta_cost_cl"
                                , []
                                , OutputFormatter.&asDynamicLookupValue.curry(securityIdentifier, lookupService, "Z_ITD_DELTA_UNITS_COST", "delta_cost", [:], ["stg_id"])
                        ),
                        new FieldValidation(
                                ["z_delta_units_cl", "z_delta_cost_cl"]
                                , "z_delta_costrate_cl"
                                , []
                                , this.&calculateDeltaCostRate
                        ),
                        new FieldValidation(
                                ["z_delta_costrate_cl"]
                                , "z_delta_billrate_cl"
                                , []
                                , OutputFormatter.&asIs
                        ),
                        new FieldValidation(
                                ["z_delta_units_cl", "z_delta_cost_cl"]
                                , "z_operation_type"
                                , []
                                , { Object... inputs -> inputs.any() ? InterfaceOperationTypeEnum.CREATE.id : InterfaceOperationTypeEnum.IGNORE.id }
                        )
                ]
        )
        rowValidation

        StagingValidation stagingValidation = new StagingValidation("Delta Units and Cost Validation", actualsInterfaceProcessor.ctx, log)
        RestResponse validationRestResponseW = stagingValidation.validate(rowValidation, InterfaceStatusEnum.WARNING, InterfaceStatusEnum.WARNING, InterfaceStatusEnum.FAILED, InterfaceStatusEnum.WARNING, InterfaceStatusEnum.VALIDATED)
        RestResponse validationRestResponseV = stagingValidation.validate(rowValidation, InterfaceStatusEnum.VALIDATED, InterfaceStatusEnum.VALIDATED, InterfaceStatusEnum.FAILED, InterfaceStatusEnum.WARNING, InterfaceStatusEnum.VALIDATED)
    }

    RowValidation initRowValidation() {
        SecurityIdentifier securityIdentifier = actualsInterfaceProcessor.ctx.restUtils.securityIdentifier
        Map<String, OdfAttribute> subobjectAttrApiToAttrMap = actualsInterfaceProcessor.ctx.stagingObjectService.subobjectAttrApiToAttrMap
        Map<String, StgToTgt> stgAttrApiToStgToTgtInstanceMap = actualsInterfaceProcessor.ctx.stgAttrApiToStgToTgtInstanceMap
        LookupService lookupService = new LookupService(sql)

        RowValidation rowValidation = new RowValidation(
                subobjectAttrApiToAttrMap,
                stgAttrApiToStgToTgtInstanceMap,
                [

                        new FieldValidation(
                                ["z_booking_date_raw"]
                                , "z_booking_date_cl"
                                ,[ValidationRule.&isNotEmpty, ValidationRule.&isDate.curry("uuuuMMdd HH:mm")]
                                , OutputFormatter.&asDate.curry("uuuuMMdd HH:mm")
                        ),

                       new FieldValidation(
                                ["z_inv_code_raw"]
                                , "z_inv_code_cl"
                                , [ValidationRule.&isNotEmpty
                                   , ValidationRule.&isValidDynamicLookupValue.curry(securityIdentifier, lookupService, "TXN_INVESTMENT_BROWSE", [:], ["code"])
                                   , ValidationRule.&isValidDynamicLookupValue.curry(securityIdentifier, lookupService, "Z_ITD_INV_ACTIVE_AND_FIN_OPEN", [:], ["code"])
                                   , ValidationRule.&isValidDynamicLookupValue.curry(securityIdentifier, lookupService, "Z_ITD_INV_DEPT_AND_LOC", [:], ["code"])
                        ]
                                , OutputFormatter.&asDynamicLookupValue.curry(securityIdentifier, lookupService, "TXN_INVESTMENT_BROWSE", "code", [:], ["code"])
                        ),
                        new FieldValidation(
                                ["z_inv_code_cl"]
                                , "z_inv_id_cl"
                                , []
                                , { Object... inputs -> inputs ? actualsInterfaceProcessor.actualsDatabaseUtils.getInvFromCode(inputs[0] as String)?.id : null }
                        ),
                        new FieldValidation(
                                ["z_desc_raw"]
                                , "z_notes_cl"
                                , []
                                , OutputFormatter.&asIs
                        ),
                        new FieldValidation(
                                ["z_cost_raw"]
                                , "z_cost_cl"
                                , [ActualsImportFromCSV.&isMoney]
                                , ActualsImportFromCSV.&moneyToNumber
                        ),
                        new FieldValidation(
                                ["z_units_raw"]
                                , "z_units_cl"
                                , []
                                , OutputFormatter.&asIs
                        ),
                        new FieldValidation(
                                ["z_res_code_raw"]
                                , "z_res_code_cl"
                                , []
                                , OutputFormatter.&asIs
                        ),

                        new FieldValidation(
                                ["z_res_code_cl"]
                                , "z_res_id_cl"
                                , []
                                , { Object... inputs -> inputs ? sql.firstRow("select id from srm_resources where unique_name = ${inputs[0]}")?.id : null }
                        ),
                        new FieldValidation(
                                ["z_res_code_cl"]
                                , "z_txn_type_cl"
                                , []
                                , { Object... inputs ->
                            if (inputs) {
                                def resourceType = sql.firstRow("select resource_type from srm_resources where unique_name = ?", [inputs[0]])?.resource_type
                                return resourceType == 0 ? "L" :
                                        resourceType == 1 ? "Q" :
                                                resourceType == 2 ? "M" :
                                                        resourceType == 3 ? "X" : null
                            }
                            return null
                        }
                        ),
                        new FieldValidation(
                                ["z_inv_code_cl", "z_task_code_raw"]
                                , "z_task_id_cl"
                                , []
                                , this.&getTaskId.curry(securityIdentifier, lookupService, "Z_ITD_TXN_TSK", [:], ["inv_code"])
                        ),
                        new FieldValidation(
                                ["z_inv_code_cl", "z_res_code_cl", "z_booking_date_cl"]
                                , "z_ext_id_cl"
                                , []
                                , this.&prepareExternalId
                        )
                ]
        )
        rowValidation
    }


    String prepareExternalId(Object... inputs) {
        if (inputs.every()) {
            String invCode = inputs[0] as String
            String resCode = inputs[1] as String
            String isoBookingDate = inputs[2] as String

            DBVendor dbVendor = DBVendor.get(sql.connection)

            return sql.firstRow("""
                select
                    niku.cmn_concat_fct(
                        niku.cmn_concat_fct(
                            niku.cmn_to_char_fct(inv.id)
                            , niku.cmn_to_char_fct(res.id)
                        )
                        , replace(${Sql.expand("${dbVendor == DBVendor.MSSQL ? "substring" : "substr"}")}(${isoBookingDate}, 1, 10), '-', '')
                    )   as ext_id
                from        inv_investments inv
                cross join  srm_resources   res
                where   inv.code = ${invCode}
                and     res.unique_name = ${resCode}
            """)?.ext_id
        }

        return null
    }

    Long getTaskId(SecurityIdentifier securityIdentifier, LookupService lookupService, String dynamicLookupType, Map<String, Object> defaultSearchCriteria, List<String> searchField, Object... inputs) {
        String invCode = inputs ? inputs[0] as String : null
        String tskcode = inputs ? inputs[1] as String :null

        log?.info "inv code ${invCode}"
        log?.info "task code ${tskcode}"

        if (!tskcode) {
            return null
        }

        defaultSearchCriteria = ["tsk_code": tskcode]

        log?.info "default search ${defaultSearchCriteria}"

        if (!invCode) {
            return null
        }

        inputs = [inputs[0]]

        Long taskId = OutputFormatter.asDynamicLookupValue(securityIdentifier, lookupService, dynamicLookupType, defaultSearchCriteria, searchField, inputs) as Long

        log?.info "taskId ${taskId}"

        if (taskId) {
            return taskId
        } else {
            Investment investment = actualsInterfaceProcessor.actualsDatabaseUtils.getInvFromCode(invCode)
            Map<String, Object> payloadMap = [
                    d: [
                            [
                                    code                : "z_actuals"
                                    , name              : "Dummy Task for Bookings"
                                    , isOpenForTimeEntry: false
                            ]
                    ]
            ]
            RestResponse restResponse = actualsInterfaceProcessor.ctx.restUtils.createTasks(investment, payloadMap)
            if (restResponse.restErrors) {
                throw new Exception(restResponse.restErrors.join("\r\n"))
            }
            return restResponse?.internalIds?.first()
        }
    }

    static BigDecimal calculateDeltaCostRate(Object... inputs) {
        BigDecimal deltaUnits = inputs[0] as BigDecimal
        BigDecimal deltaCost = inputs[1] as BigDecimal
        return deltaUnits ? deltaCost / deltaUnits : deltaCost
    }

    static Validation isMoney(String... inputs) {
        String value = inputs[0]
        return value ? ValidationRule.isNumber(Locale.GERMAN, value.replaceAll("€", "")) : Validation.valid()
    }

    static BigDecimal moneyToNumber(String... inputs) {
        String value = inputs[0]
        return value ? asBigDecimal(inputs[0].replaceAll("€", "")) : BigDecimal.ZERO
    }

    static BigDecimal asBigDecimal(String amount) {
        try {
            final NumberFormat format = NumberFormat.getNumberInstance(Locale.GERMANY)
            if (format instanceof DecimalFormat) {
                ((DecimalFormat) format).setParseBigDecimal(true)
            }
            return (BigDecimal) format.parse(amount.trim())
        } catch (ParseException ignored) {
            return BigDecimal.ZERO
        }
    }
}