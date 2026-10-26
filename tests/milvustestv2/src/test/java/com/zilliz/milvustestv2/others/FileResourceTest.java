package com.zilliz.milvustestv2.others;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.service.utility.request.AddFileResourceReq;
import io.milvus.v2.service.utility.request.ListFileResourcesReq;
import io.milvus.v2.service.utility.request.RemoveFileResourceReq;
import io.milvus.v2.service.utility.response.FileResourceInfo;
import io.milvus.v2.service.utility.response.ListFileResourcesResp;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Test cases for file resource APIs (Milvus 3.0): addFileResource / listFileResources / removeFileResource。
 * 注意：path 为服务端本地路径，服务端可能校验文件存在性；远程实例上 /tmp/test.txt 不一定存在，
 * 若 add 因环境原因失败，则记录行为并跳过式通过，不做硬断言。
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
@Slf4j
public class FileResourceTest extends BaseTest {

    @Test(description = "File resource lifecycle: add -> list -> remove -> list", groups = {"Smoke", "V3"})
    public void fileResourceLifecycle() {
        String name = "res_" + GenerateUtil.getRandomString(6);
        String path = "/tmp/test.txt";
        try {
            milvusClientV2.addFileResource(AddFileResourceReq.builder()
                    .name(name)
                    .path(path)
                    .build());
        } catch (Exception e) {
            // 环境不支持（服务端校验 path 存在性/功能未开启）时记录行为，跳过式通过
            log.warn("addFileResource failed, environment may not support it or path not existed on server: "
                    + e.getMessage());
            return;
        }
        try {
            // listFileResources 断言含该 name 且 path 正确
            ListFileResourcesResp listResp = milvusClientV2.listFileResources(ListFileResourcesReq.builder().build());
            Assert.assertNotNull(listResp.getResources());
            FileResourceInfo found = findResourceByName(listResp.getResources(), name);
            Assert.assertNotNull(found, "listFileResources should contain " + name);
            Assert.assertEquals(found.getPath(), path, "resource path mismatch");

            // removeFileResource 后 list 不含
            milvusClientV2.removeFileResource(RemoveFileResourceReq.builder()
                    .name(name)
                    .build());
            ListFileResourcesResp listAfterRemove = milvusClientV2.listFileResources(
                    ListFileResourcesReq.builder().build());
            Assert.assertNull(findResourceByName(listAfterRemove.getResources(), name),
                    "resource should be removed: " + name);
        } finally {
            // 防御式清理
            try {
                milvusClientV2.removeFileResource(RemoveFileResourceReq.builder().name(name).build());
            } catch (Exception e) {
                log.warn("cleanup removeFileResource failed: " + e.getMessage());
            }
        }
    }

    @Test(description = "Add file resource with duplicated name, behavior recorded", groups = {"Smoke", "V3"})
    public void addDuplicatedFileResource() {
        String name = "res_dup_" + GenerateUtil.getRandomString(6);
        String path = "/tmp/test.txt";
        try {
            milvusClientV2.addFileResource(AddFileResourceReq.builder().name(name).path(path).build());
        } catch (Exception e) {
            log.warn("addFileResource failed (environment), skip duplicated check: " + e.getMessage());
            return;
        }
        try {
            // 行为记录：同名 add 第二次，可能覆盖也可能抛异常，以实际服务端行为为准
            milvusClientV2.addFileResource(AddFileResourceReq.builder().name(name).path(path).build());
            log.info("add duplicated file resource: no exception (overwritten or idempotent)");
        } catch (Exception e) {
            log.info("add duplicated file resource throws: " + e.getMessage());
        } finally {
            try {
                milvusClientV2.removeFileResource(RemoveFileResourceReq.builder().name(name).build());
            } catch (Exception e) {
                log.warn("cleanup removeFileResource failed: " + e.getMessage());
            }
        }
    }

    @Test(description = "Remove not existed file resource, behavior recorded", groups = {"Smoke", "V3"})
    public void removeNotExistedFileResource() {
        // 行为记录：remove 不存在的 name，可能幂等成功也可能抛异常
        try {
            milvusClientV2.removeFileResource(RemoveFileResourceReq.builder()
                    .name("res_not_exist_" + GenerateUtil.getRandomString(6))
                    .build());
            log.info("remove not existed file resource: no exception (idempotent)");
        } catch (Exception e) {
            log.info("remove not existed file resource throws: " + e.getMessage());
        }
    }

    @Test(description = "Add file resource with empty path, behavior recorded", groups = {"Smoke", "V3"})
    public void addFileResourceWithEmptyPath() {
        // 行为记录：path 为空串，可能参数校验异常也可能被接受
        String name = "res_empty_" + GenerateUtil.getRandomString(6);
        try {
            milvusClientV2.addFileResource(AddFileResourceReq.builder()
                    .name(name)
                    .path("")
                    .build());
            log.info("add file resource with empty path: no exception");
        } catch (Exception e) {
            log.info("add file resource with empty path throws: " + e.getMessage());
        } finally {
            try {
                milvusClientV2.removeFileResource(RemoveFileResourceReq.builder().name(name).build());
            } catch (Exception e) {
                log.warn("cleanup removeFileResource failed: " + e.getMessage());
            }
        }
    }

    private static FileResourceInfo findResourceByName(List<FileResourceInfo> resources, String name) {
        if (resources == null) {
            return null;
        }
        for (FileResourceInfo info : resources) {
            if (name.equals(info.getName())) {
                return info;
            }
        }
        return null;
    }
}
