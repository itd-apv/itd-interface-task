import de.itdesign.interfaces.validation.FieldValidation
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import groovy.sql.Sql
import groovy.sql.GroovyRowResult
import static org.mockito.Mockito.*
import static org.junit.jupiter.api.Assertions.*

class FieldValidationTest {

    @Test
    void testResourceTypeMapping() {
        // Mock the SQL object
        def sqlMock = mock(Sql) // Assuming Sql is used to query the database

        // Create a GroovyRowResult mock to simulate the return of firstRow()
        def groovyRowResult = new GroovyRowResult([resource_type: 0]) // Mock the GroovyRowResult

        // Define the inputs for the field validation
        def validation = new FieldValidation(
                ["z_res_code_cl"],
                "z_txn_type_cl",
                [],
                { Object... inputs ->
                    if (inputs) {
                        // Mock the SQL response based on the input value
                        def resourceType = sqlMock.firstRow("select resource_type from srm_resources where unique_name = ?", [inputs[0]])?.resource_type
                        return resourceType == 0 ? "L" :
                                resourceType == 1 ? "Q" :
                                        resourceType == 2 ? "M" :
                                                resourceType == 3 ? "X" : null
                    }
                    return null
                }
        )

        // Mocking SQL query response to return the GroovyRowResult with resource_type
        when(sqlMock.firstRow("select resource_type from srm_resources where unique_name = ?", ["some_unique_name"])).thenReturn(groovyRowResult)

        // Test the logic with mocked SQL response
        def result = validation.validate("some_unique_name")  // Call the correct method, assuming it's 'validate'
        assertEquals("L", result)

        // Test for other resource types
        groovyRowResult = new GroovyRowResult([resource_type: 1]) // Resource type 1 should map to "Q"
        when(sqlMock.firstRow("select resource_type from srm_resources where unique_name = ?", ["another_unique_name"])).thenReturn(groovyRowResult)
        result = validation.validate("another_unique_name")  // Call the correct method
        assertEquals("Q", result)

        groovyRowResult = new GroovyRowResult([resource_type: 2]) // Resource type 2 should map to "M"
        when(sqlMock.firstRow("select resource_type from srm_resources where unique_name = ?", ["third_unique_name"])).thenReturn(groovyRowResult)
        result = validation.validate("third_unique_name")  // Call the correct method
        assertEquals("M", result)

        groovyRowResult = new GroovyRowResult([resource_type: 3]) // Resource type 3 should map to "X"
        when(sqlMock.firstRow("select resource_type from srm_resources where unique_name = ?", ["fourth_unique_name"])).thenReturn(groovyRowResult)
        result = validation.validate("fourth_unique_name")  // Call the correct method
        assertEquals("X", result)

        groovyRowResult = new GroovyRowResult([resource_type: 99]) // Invalid resource type
        when(sqlMock.firstRow("select resource_type from srm_resources where unique_name = ?", ["unknown_unique_name"])).thenReturn(groovyRowResult)
        result = validation.validate("unknown_unique_name")  // Call the correct method
        assertNull(result) // Should return null for invalid resource type
    }
}
