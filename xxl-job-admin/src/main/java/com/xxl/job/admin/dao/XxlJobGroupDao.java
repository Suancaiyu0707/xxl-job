package com.xxl.job.admin.dao;

import com.xxl.job.admin.core.model.XxlJobGroup;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * Created by xuxueli on 16/9/30.
 */
@Mapper
public interface XxlJobGroupDao {
    //查询 执行器列表
    public List<XxlJobGroup> findAll();

    public List<XxlJobGroup> findByAddressType(@Param("addressType") int addressType);

    public int save(XxlJobGroup xxlJobGroup);

    /***
     *   注意，如果这个执行器是自动注册的话，则地址是用客户端执行器注册保存的。客户端执行器在注册完保存到xxlJobRegistry之后，
     *   JobRegistryMonitorHelper的线程会每隔30s把xxlJobRegistry里的注册的执行器信息，按照appName分组后，更新到xxl-job-group表中
     * @param xxlJobGroup
     * @return
     */
    public int update(XxlJobGroup xxlJobGroup);

    public int remove(@Param("id") int id);

    public XxlJobGroup load(@Param("id") int id);
}
