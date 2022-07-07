package com.panda.system.repository;


import com.panda.system.domin.SysUser;
import com.panda.system.domin.vo.SysUserVo;
import com.panda.system.exception.RepositoryConflictException;
import com.panda.system.exception.RepositoryCrudException;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.springframework.stereotype.Repository;

@Repository
public class UserRepository extends ScalarDbReadOnlyRepository<SysUser> {
  public static final String NAMESPACE = "demo";
  public static final String TABLE_NAME = "users";
  public static final String COMMON_KEY = "common_key";

  public String createUser(DistributedTransaction tx, SysUserVo createUserDto, String userId) {
    try {
      Key pk = createPk(userId);
      getAndThrowsIfAlreadyExist(tx, createGet(pk));
      Put put =null;
//          new Put(pk)
//              .forNamespace(NAMESPACE)
//              .forTable(TABLE_NAME)
//              .withValue(User.EMAIL, createUserDto.getEmail())
//              .withValue(User.COMMON_KEY, COMMON_KEY);
      tx.put(put);
      return userId;
    } catch (CrudConflictException e) {
      throw new RepositoryConflictException(e.getMessage(), e);
    } catch (CrudException e) {
      throw new RepositoryCrudException("Adding User failed", e);
    }
  }

//  public void updateUser(
//      DistributedTransaction tx,
//      UpdateUserDto updateUserDto,
//      List<GetGroupDto> userGroups,
//      String userId)
//      throws CrudException {
//    try {
//      Key pk = createPk(userId);
//      getAndThrowsIfNotFound(tx, createGet(pk));
//      Put put =
//          new Put(pk)
//              .forNamespace(NAMESPACE)
//              .forTable(TABLE_NAME)
//              .withValue(User.EMAIL, updateUserDto.getEmail())
//              .withValue(User.FAMILY_NAME, updateUserDto.getFamilyName())
//              .withValue(User.GIVEN_NAME, updateUserDto.getGivenName())
//              .withValue(
//                  User.USER_DETAIL,
//                  ScalarUtil.convertDataObjectToJsonStr(updateUserDto.getUserDetail()))
//              .withValue(User.USER_GROUPS, ScalarUtil.convertDataObjectToJsonStr(userGroups))
//              .withValue(User.COMMON_KEY, COMMON_KEY);
//      tx.put(put);
//    } catch (CrudConflictException e) {
//      throw new RepositoryConflictException(e.getMessage(), e);
//    } catch (CrudException e) {
//      throw new RepositoryCrudException("Updating User failed", e);
//    }
//  }

//  public void updateUserGroups(
//      DistributedTransaction tx, String userId, List<UserGroup> userGroups) {
//    try {
//      Key pk = createPk(userId);
//      User user = getAndThrowsIfNotFound(tx, createGet(pk));
//      Put put =
//          new Put(pk)
//              .forNamespace(NAMESPACE)
//              .forTable(TABLE_NAME)
//              .withValue(User.EMAIL, user.getEmail())
//              .withValue(User.FAMILY_NAME, user.getFamilyName())
//              .withValue(User.GIVEN_NAME, user.getGivenName())
//              .withValue(
//                  User.USER_DETAIL, ScalarUtil.convertDataObjectToJsonStr(user.getUserDetail()))
//              .withValue(User.USER_GROUPS, ScalarUtil.convertDataObjectToJsonStr(userGroups))
//              .withValue(User.COMMON_KEY, COMMON_KEY);
//      tx.put(put);
//    } catch (CrudConflictException e) {
//      throw new RepositoryConflictException(e.getMessage(), e);
//    } catch (CrudException e) {
//      throw new RepositoryCrudException("Updating UserGroups failed", e);
//    }
//  }

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

  public List<SysUser> listUsers(DistributedTransaction tx) {
    try {
      Scan scan = createScanWithCommonKey();
      return scan(tx, scan);
    } catch (CrudConflictException e) {
      throw new RepositoryConflictException(e.getMessage(), e);
    } catch (CrudException e) {
      throw new RepositoryCrudException("Listing Users failed", e);
    }
  }

  public SysUser getUser(DistributedTransaction tx, String userId) {
    try {
      Key pk = createPk(userId);
      return getAndThrowsIfNotFound(tx, createGet(pk));
    } catch (CrudConflictException e) {
      throw new RepositoryConflictException(e.getMessage(), e);
    } catch (CrudException e) {
      throw new RepositoryCrudException("Reading User failed", e);
    }
  }

  private Key createPk(String userId) {
    return new Key(new TextValue(SysUser.USER_ID, userId));
  }

  private Get createGet(Key pk) {
    return new Get(pk).forNamespace(NAMESPACE).forTable(TABLE_NAME);
  }

  private Scan createScanWithCommonKey() {
    return new Scan(new Key(new TextValue(SysUser.COMMON_KEY, COMMON_KEY)))
        .forNamespace(NAMESPACE)
        .forTable(TABLE_NAME);
  }

  @Override
  SysUser parse(@NotNull Result result) {
      return  null;
//    UserBuilder builder = SysUser.builder();
//    return builder
//        .userId(ScalarUtil.getTextValue(result, SysUser.USER_ID))
//        .email(ScalarUtil.getTextValue(result, SysUser.EMAIL))
//        .familyName(ScalarUtil.getTextValue(result, SysUser.FAMILY_NAME))
//        .givenName(ScalarUtil.getTextValue(result, SysUser.GIVEN_NAME))
//        .userGroups(
//            ScalarUtil.convertJsonStrToDataObjectList(
//                ScalarUtil.getTextValue(result, SysUser.USER_GROUPS), UserGroup[].class))
//        .userDetail(
//            ScalarUtil.convertJsonStrToDataObject(
//                ScalarUtil.getTextValue(result, SysUser.USER_DETAIL), UserDetail.class))
//        .build();
  }
}
