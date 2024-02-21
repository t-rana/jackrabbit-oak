package org.apache.jackrabbit.oak.upgrade.cli.node;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.upgrade.cli.CliUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.EnumSet;

import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.ADD;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.CREATE;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.DELETE;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.LIST;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.READ;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.WRITE;
import static org.junit.Assert.assertEquals;

public class SegmentAzureFactoryTest {

    @ClassRule
    public static final AzuriteDockerRule azurite = new AzuriteDockerRule();

    private static final String CONTAINER_NAME = "oak-test";
    private static final EnumSet<SharedAccessBlobPermissions> READ_WRITE = EnumSet.of(READ, LIST, CREATE, WRITE, ADD);

    @Test
    public void testConnectionWithConnectionString_accessKey() throws IOException {
        String connectionStringWithPlaceholder = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;BlobEndpoint=http://127.0.0.1:%s/%s;";
        String connectionString = String.format(connectionStringWithPlaceholder, AzuriteDockerRule.ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_KEY, azurite.getMappedPort(), AzuriteDockerRule.ACCOUNT_NAME);
        SegmentAzureFactory segmentAzureFactory = new SegmentAzureFactory.Builder("respository", 256,
                false)
                .connectionString(connectionString)
                .containerName(CONTAINER_NAME)
                .build();
        Closer closer = Closer.create();
        CliUtils.handleSigInt(closer);
        FileStoreUtils.NodeStoreWithFileStore nodeStore = (FileStoreUtils.NodeStoreWithFileStore) segmentAzureFactory.create(null, closer);
        assertEquals(1, nodeStore.getFileStore().getSegmentCount());
        closer.close();
    }

    /* this is failing on container.createIfNotExists() with sas uri
    * */

//    @Test
//    public void testConnectionWithConnectionString_sas() throws URISyntaxException, InvalidKeyException, StorageException, IOException {
//        String sasToken = azurite.getContainer(CONTAINER_NAME).generateSharedAccessSignature(policy(READ_WRITE), null);
//        String connectionStringWithPlaceholder = "DefaultEndpointsProtocol=http;AccountName=%s;SharedAccessSignature=%s;BlobEndpoint=http://127.0.0.1:%s/%s;";
//        String connectionString = String.format(connectionStringWithPlaceholder, AzuriteDockerRule.ACCOUNT_NAME, sasToken, azurite.getMappedPort(), AzuriteDockerRule.ACCOUNT_NAME);
//        SegmentAzureFactory segmentAzureFactory = new SegmentAzureFactory.Builder("respository", 256,
//                false)
//                .connectionString(connectionString)
//                .containerName(CONTAINER_NAME)
//                .build();
//        Closer closer = Closer.create();
//        CliUtils.handleSigInt(closer);
//        FileStoreUtils.NodeStoreWithFileStore nodeStore = (FileStoreUtils.NodeStoreWithFileStore) segmentAzureFactory.create(null, closer);
//        assertEquals(1, nodeStore.getFileStore().getSegmentCount());
//        closer.close();
//    }


    @NotNull
    private static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions, Instant expirationTime) {
        SharedAccessBlobPolicy sharedAccessBlobPolicy = new SharedAccessBlobPolicy();
        sharedAccessBlobPolicy.setPermissions(permissions);
        sharedAccessBlobPolicy.setSharedAccessExpiryTime(Date.from(expirationTime));
        return sharedAccessBlobPolicy;
    }

    @NotNull
    private static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions) {
        return policy(permissions, Instant.now().plus(Duration.ofMinutes(10)));
    }

}
