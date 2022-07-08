package com.panda.system.repository;


import com.panda.system.domin.SysRole;
import com.panda.system.domin.SysUser;
import com.panda.system.domin.vo.SysUserVo;
import com.panda.system.exception.RepositoryConflictException;
import com.panda.system.exception.RepositoryCrudException;
import com.panda.system.util.ScalarUtil;
import com.scalar.db.api.*;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import org.springframework.stereotype.Repository;

import javax.management.relation.Role;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Optional;

/**
 * https://github.com/scalar-labs/scalardb/blob/master/docs/api-guide.md
 */
@Repository
public class RoleRepository extends ScalarDbReadOnlyRepository<SysRole> {
  public static final String NAMESPACE = "movie";
  public static final String TABLE_NAME = "sys_role3";
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

  /**
   * TODO 执行下面的语句可以查询成功
   * java -jar scalardb-schema-loader-3.5.3.jar --config database.properties -f schema.json
   */
  public SysRole getRole(DistributedTransaction tx, String roleId) {
    try {
      Key pk = new Key("id", 1); //这个是描述表schema里定义的partition key
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

  /**
   * java.lang.IllegalArgumentException: The specified table is not found: coordinator.state
   * 	at com.scalar.db.storage.common.checker.OperationChecker.getMetadata(OperationChecker.java:202)
   * 	at com.scalar.db.storage.common.checker.OperationChecker.check(OperationChecker.java:34)
   * 	at com.scalar.db.storage.jdbc.JdbcService.get(JdbcService.java:56)
   * 	at com.scalar.db.storage.jdbc.JdbcDatabase.get(JdbcDatabase.java:80)
   * 	at com.scalar.db.transaction.consensuscommit.Coordinator.get(Coordinator.java:84)
   * 	at com.scalar.db.transaction.consensuscommit.Coordinator.getState(Coordinator.java:62)
   * 	at com.scalar.db.transaction.consensuscommit.RecoveryHandler.recover(RecoveryHandler.java:63)
   * 	at com.scalar.db.transaction.consensuscommit.ConsensusCommit.lambda$lazyRecovery$5(ConsensusCommit.java:160)
   * 	at com.google.common.collect.ImmutableList.forEach(ImmutableList.java:406)
   * 	at com.scalar.db.transaction.consensuscommit.ConsensusCommit.lazyRecovery(ConsensusCommit.java:160)
   * 	at com.scalar.db.transaction.consensuscommit.ConsensusCommit.get(ConsensusCommit.java:66)
   * 	at com.panda.system.repository.RoleRepository.getRole(RoleRepository.java:85)
   * 	at com.panda.system.repository.RoleRepository$$FastClassBySpringCGLIB$$51df6b46.invoke(<generated>)
   * 	at org.springframework.cglib.proxy.MethodProxy.invoke(MethodProxy.java:218)
   * 	at org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.invokeJoinpoint(CglibAopProxy.java:771)
   * 	at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:163)
   * 	at org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.proceed(CglibAopProxy.java:749)
   * 	at org.springframework.dao.support.PersistenceExceptionTranslationInterceptor.invoke(PersistenceExceptionTranslationInterceptor.java:137)
   * 	at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:186)
   * 	at org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.proceed(CglibAopProxy.java:749)
   * 	at org.springframework.aop.framework.CglibAopProxy$DynamicAdvisedInterceptor.intercept(CglibAopProxy.java:691)
   * 	at com.panda.system.repository.RoleRepository$$EnhancerBySpringCGLIB$$1d92b3de.getRole(<generated>)
   * 	at com.panda.system.service.impl.SysRoleServiceImpl.findRoleById(SysRoleServiceImpl.java:64)
   * 	at com.panda.web.controller.system.SysRoleController.findRoleById(SysRoleController.java:28)
   * 	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
   * 	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
   * 	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
   * 	at java.lang.reflect.Method.invoke(Method.java:498)
   * 	at org.springframework.web.method.support.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:197)
   * 	at org.springframework.web.method.support.InvocableHandlerMethod.invokeForRequest(InvocableHandlerMethod.java:141)
   * 	at org.springframework.web.servlet.mvc.method.annotation.ServletInvocableHandlerMethod.invokeAndHandle(ServletInvocableHandlerMethod.java:106)
   * 	at org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.invokeHandlerMethod(RequestMappingHandlerAdapter.java:893)
   * 	at org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.handleInternal(RequestMappingHandlerAdapter.java:807)
   * 	at org.springframework.web.servlet.mvc.method.AbstractHandlerMethodAdapter.handle(AbstractHandlerMethodAdapter.java:87)
   * 	at org.springframework.web.servlet.DispatcherServlet.doDispatch(DispatcherServlet.java:1061)
   * 	at org.springframework.web.servlet.DispatcherServlet.doService(DispatcherServlet.java:961)
   * 	at org.springframework.web.servlet.FrameworkServlet.processRequest(FrameworkServlet.java:1006)
   * 	at org.springframework.web.servlet.FrameworkServlet.doGet(FrameworkServlet.java:898)
   * 	at javax.servlet.http.HttpServlet.service(HttpServlet.java:626)
   * 	at org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:883)
   * 	at javax.servlet.http.HttpServlet.service(HttpServlet.java:733)
   * 	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:231)
   * 	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:166)
   * 	at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:53)
   * 	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:193)
   * 	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:166)
   * 	at org.apache.shiro.web.servlet.AbstractShiroFilter.executeChain(AbstractShiroFilter.java:449)
   * 	at org.apache.shiro.web.servlet.AbstractShiroFilter$1.call(AbstractShiroFilter.java:365)
   * 	at org.apache.shiro.subject.support.SubjectCallable.doCall(SubjectCallable.java:90)
   * 	at org.apache.shiro.subject.support.SubjectCallable.call(SubjectCallable.java:83)
   * 	at org.apache.shiro.subject.support.DelegatingSubject.execute(DelegatingSubject.java:387)
   * 	at org.apache.shiro.web.servlet.AbstractShiroFilter.doFilterInternal(AbstractShiroFilter.java:362)
   * 	at org.apache.shiro.web.servlet.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:125)
   * 	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:193)
   * 	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:166)
   * 	at org.apache.shiro.web.servlet.AdviceFilter.executeChain(AdviceFilter.java:108)
   * 	at org.apache.shiro.web.servlet.AdviceFilter.doFilterInternal(AdviceFilter.java:137)
   * 	at org.apache.shiro.web.servlet.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:125)
   * 	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:193)
   * 	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:166)
   * 	at org.springframework.web.filter.RequestContextFilter.doFilterInternal(RequestContextFilter.java:100)
   * 	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:119)
   * 	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:193)
   * 	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:166)
   * 	at org.springframework.web.filter.FormContentFilter.doFilterInternal(FormContentFilter.java:93)
   * 	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:119)
   * 	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:193)
   * 	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:166)
   * 	at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:201)
   * 	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:119)
   * 	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:193)
   * 	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:166)
   * 	at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:202)
   * 	at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:97)
   * 	at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:542)
   * 	at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:143)
   * 	at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:92)
   * 	at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:78)
   * 	at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:343)
   * 	at org.apache.coyote.http11.Http11Processor.service(Http11Processor.java:374)
   * 	at org.apache.coyote.AbstractProcessorLight.process(AbstractProcessorLight.java:65)
   * 	at org.apache.coyote.AbstractProtocol$ConnectionHandler.process(AbstractProtocol.java:868)
   * 	at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1590)
   * 	at org.apache.tomcat.util.net.SocketProcessorBase.run(SocketProcessorBase.java:49)
   * 	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
   * 	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
   * 	at org.apache.tomcat.util.threads.TaskThread$WrappingRunnable.run(TaskThread.java:61)
   * 	at java.lang.Thread.run(Thread.java:748)
   * @param roleId
   * @return
   */

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
