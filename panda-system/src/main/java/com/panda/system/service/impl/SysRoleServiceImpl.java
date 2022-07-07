package com.panda.system.service.impl;

import com.panda.system.domin.SysRole;
import com.panda.system.exception.RepositoryConflictException;
import com.panda.system.exception.RepositoryCrudException;
import com.panda.system.exception.ServiceException;
import com.panda.system.mapper.SysRoleMapper;
import com.panda.system.repository.RoleRepository;
import com.panda.system.service.SysRoleService;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.UUID;


@Service
public class SysRoleServiceImpl implements SysRoleService {

    @Autowired
    private DistributedTransactionManager manager;

    @Autowired
    private RoleRepository roleRepository;

    private DistributedTransaction tx;

    @Autowired
    private SysRoleMapper sysRoleMapper;

    @Override
    public List<SysRole> findAllRoles() {
        return sysRoleMapper.findAllRoles();
    }

    @Override
    public SysRole findRoleById(Long id) {
//        return sysRoleMapper.findRoleById(id);
        while (true) {
//            if (retryCount > 0) {
//                if (retryCount == 3) {
//                    throw new ServiceException("An error occurred when adding a user");
//                }
//                TimeUnit.MILLISECONDS.sleep(100);
//            }

            try {
                tx = manager.start();
            } catch (TransactionException e) {
                try {
                    tx.abort();
                } catch (AbortException ex) {
//                    log.error(ex.getMessage(), ex);
                }
                throw new ServiceException("An error occurred when adding a group", e);
            }

            try {
                String userId = UUID.randomUUID().toString();
                SysRole role = roleRepository.getRole(tx, "1");
                tx.commit();
                return role;
            } catch (CommitConflictException | RepositoryConflictException e) {
                try {
                    tx.abort();
                } catch (AbortException ex) {
//                    log.erroror(ex.getMessage(), ex);
                }
//                retryCount++;
            } catch (CommitException | RepositoryCrudException | UnknownTransactionStatusException e) {
                try {
                    tx.abort();
                } catch (AbortException ex) {
//                    log.error(exception.getMessage(), ex);
                }
                throw new ServiceException("An error occurred when adding a user", (Throwable) e);
            }
        }
    }

    int retryCount = 0;

    @Override
    public int addRole(SysRole sysRole) {
//        return sysRoleMapper.addRole(sysRole);
        while (true) {
//            if (retryCount > 0) {
//                if (retryCount == 3) {
//                    throw new ServiceException("An error occurred when adding a user");
//                }
//                TimeUnit.MILLISECONDS.sleep(100);
//            }

            try {
                tx = manager.start();
            } catch (TransactionException e) {
                try {
                    tx.abort();
                } catch (AbortException ex) {
//                    log.error(ex.getMessage(), ex);
                }
                throw new ServiceException("An error occurred when adding a group", e);
            }

            try {
                String userId = UUID.randomUUID().toString();
                roleRepository.createRole(tx, sysRole,userId);
                tx.commit();
                return 0;
            } catch (CommitConflictException | RepositoryConflictException e) {
                try {
                    tx.abort();
                } catch (AbortException ex) {
//                    log.erroror(ex.getMessage(), ex);
                }
//                retryCount++;
            } catch (CommitException | RepositoryCrudException | UnknownTransactionStatusException e) {
                try {
                    tx.abort();
                } catch (AbortException ex) {
//                    log.error(exception.getMessage(), ex);
                }
                throw new ServiceException("An error occurred when adding a user", (Throwable) e);
            }
        }
    }

    @Override
    public int updateRole(SysRole sysRole) {
        return sysRoleMapper.updateRole(sysRole);
    }

    @Override
    public int deleteRole(Long[] ids) {
        int rows = 0;
        for (Long id : ids) {
            rows += sysRoleMapper.deleteRole(id);
        }
        return rows;
    }

    @Override
    public int allotRight(Long roleId, Long[] keys) {
        int rows = 0;
        HashSet<Long> originResources = new HashSet<>(sysRoleMapper.findAllRights(roleId));

        for (Long id : keys) {
            if (originResources.contains(id)) {
                originResources.remove(id);
            } else {
                rows += sysRoleMapper.addRight(roleId, id);
            }
        }
        for (Long id : originResources) {
            rows += sysRoleMapper.deleteRight(roleId, id);
        }
        return rows;
    }
}
