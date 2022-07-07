package com.panda.system.domin;

import lombok.*;

import javax.validation.constraints.NotBlank;
import java.io.Serializable;
import java.util.List;


@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Builder
public class SysRole implements Serializable {

    private static final Long serialVersionUID = 1L;

    public static final String ROLE_NAME = "role_name";
    public static final String ROLE_ID = "role_id";
    public static final String ROLE_DESC = "role_desc";

    private Long roleId;

    //角色名称
    @NotBlank(message = "角色名称不能为空")
    public String roleName;

    //角色描述
    @NotBlank(message = "角色描述不能为空")
    public String roleDesc;


    //角色拥有的权限，分多级权限存储，取名为children方便读取所有权限
    public List<SysResource> children;
}
