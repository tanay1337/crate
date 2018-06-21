package io.crate.iothub.operations;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import io.crate.action.sql.SQLOperations;
import io.crate.auth.user.UserLookup;
import io.crate.ingestion.IngestionService;
import io.crate.metadata.Functions;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class EventIngestServiceTest extends CrateUnitTest {

    private EventIngestService subjectUnderTest;

    @Before
    public void setup() {
        subjectUnderTest = new EventIngestService(
            mock(Functions.class), mock(SQLOperations.class), mock(UserLookup.class), mock(IngestionService.class)
        );
    }

    @Test
    public void initialize_givenServiceInitializedTwice_thenThrowsIllegalStateException() throws Exception {
        subjectUnderTest.initalize();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("IoT Hub Ingestion Service already initialized.");
        subjectUnderTest.initalize();
    }

    @Test
    public void onInsert_givenDoInsertOnUninitializedService_thenThrowsIllegalStateException() throws Exception {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("IoT Hub Ingestion Service has not been initialized");
        subjectUnderTest.doInsert(mock(PartitionContext.class), mock(EventData.class));
    }

}
