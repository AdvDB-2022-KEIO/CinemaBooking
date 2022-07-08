package com.panda.system.repository;


import com.panda.system.domin.SysRole;
import com.panda.system.domin.SysUser;
import com.panda.system.exception.RepositoryConflictException;
import com.panda.system.exception.RepositoryCrudException;
import com.panda.system.util.ScalarUtil;
import com.scalar.db.api.*;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import org.springframework.stereotype.Repository;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Optional;

/**
 * https://github.com/scalar-labs/scalardb/blob/master/docs/api-guide.md
 */
@Repository
public class MovieRepository extends ScalarDbReadOnlyRepository<SysRole> {
  public static final String NAMESPACE = "movie";
  public static final String TABLE_NAME = "sys_role";
  public static final String COMMON_KEY = "common_key";

  public String createRole(DistributedTransaction tx, SysRole roleDto,String userId) {
    try {
      Key pk = createPk(userId);
      getAndThrowsIfAlreadyExist(tx, createGet(pk));
      Put put =null;
          new Put(pk)
              .forNamespace(NAMESPACE)
              .forTable(TABLE_NAME)
              .withValue(SysRole.ROLE_NAME,roleDto.roleName)
              .withValue(SysRole.ROLE_DESC,roleDto.roleDesc);
      tx.put(put);
      return userId;
    } catch (CrudConflictException e) {
      throw new RepositoryConflictException(e.getMessage(), e);
    } catch (CrudException e) {
      throw new RepositoryCrudException("Adding User failed", e);
    }
  }

  public void deleteUser(DistributedTransaction tx, String userId) {
    try {
      Key pk = createPk(userId);
      getAndThrowsIfNotFound(tx, createGet(pk));
      Delete delete = new Delete(pk).forNamespace(NAMESPACE).forTable(TABLE_NAME);
      tx.delete(delete);
    } catch (CrudConflictException e) {
      throw new RepositoryConflictException(e.getMessage(), e);
    } catch (CrudException e) {
      throw new RepositoryCrudException("Deleting User failed", e);
    }
  }

  public List<SysRole> listUsers(DistributedTransaction tx) {
    try {
      Scan scan = createScanWithCommonKey();
      return scan(tx, scan);
    } catch (CrudConflictException e) {
      throw new RepositoryConflictException(e.getMessage(), e);
    } catch (CrudException e) {
      throw new RepositoryCrudException("Listing Users failed", e);
    }
  }

  public SysRole getRole(DistributedTransaction tx, String roleId) {
    try {
      Key pk = new Key(new TextValue(SysRole.ROLE_ID, roleId));
      Get get= new Get(pk)
              .forNamespace(NAMESPACE)
              .forTable(TABLE_NAME);
      Optional<Result> result = tx.get(get);
      SysRole role=new SysRole();
      role.roleDesc=ScalarUtil.getTextValue(result.get(), SysRole.ROLE_DESC);
      role.roleName=ScalarUtil.getTextValue(result.get(), SysRole.ROLE_NAME);
      return  role;
    } catch (CrudConflictException e) {
      throw new RepositoryConflictException(e.getMessage(), e);
    } catch (CrudException e) {
      throw new RepositoryCrudException("Reading User failed", e);
    }
  }

  private Key createPk(String roleId) {
    return new Key(new TextValue(SysRole.ROLE_ID, roleId));
  }

  private Get createGet(Key pk) {
//    Get get =
//            Get.newBuilder()
//                    .namespace("ns")
//                    .table("tbl")
//                    .indexKey(indexKey)
//                    .projections("c1", "c2", "c3", "c4")
//                    .build();
    return new Get(pk).forNamespace(NAMESPACE).forTable(TABLE_NAME);
  }

  private Scan createScanWithCommonKey() {
    return new Scan(new Key(new TextValue(SysUser.COMMON_KEY, COMMON_KEY)))
        .forNamespace(NAMESPACE)
        .forTable(TABLE_NAME);
  }

  @Override
  SysRole parse(@NotNull Result result) {
    SysRole role=new SysRole();
    role.roleDesc=ScalarUtil.getTextValue(result, SysRole.ROLE_DESC);
    role.roleName=ScalarUtil.getTextValue(result, SysRole.ROLE_NAME);
    return  role;
  }
}
