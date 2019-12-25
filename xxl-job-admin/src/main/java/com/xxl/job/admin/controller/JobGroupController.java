package com.xxl.job.admin.controller;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobRegistry;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.admin.dao.XxlJobGroupDao;
import com.xxl.job.admin.dao.XxlJobInfoDao;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * job group controller
 * @author xuxueli 2016-10-02 20:52:56
 * 执行器管理
 */
@Controller
@RequestMapping("/jobgroup")
public class JobGroupController {

	@Resource
	public XxlJobInfoDao xxlJobInfoDao;
	@Resource
	public XxlJobGroupDao xxlJobGroupDao;

	/***
	 * 查询执行器列表
	 * @param model
	 * @return
	 */
	@RequestMapping
	public String index(Model model) {

		// 默认查询所有的执行器 xxl_job_group
		List<XxlJobGroup> list = xxlJobGroupDao.findAll();

		model.addAttribute("list", list);
		return "jobgroup/jobgroup.index";
	}

	/***
	 * 添加一个执行器
	 * @param xxlJobGroup
	 * @return
	 */
	@RequestMapping("/save")
	@ResponseBody
	public ReturnT<String> save(XxlJobGroup xxlJobGroup){

		// AppName不能为空，且长度是大于3，小于21
		if (xxlJobGroup.getAppName()==null || xxlJobGroup.getAppName().trim().length()==0) {
			return new ReturnT<String>(500, (I18nUtil.getString("system_please_input")+"AppName") );
		}
		if (xxlJobGroup.getAppName().length()<4 || xxlJobGroup.getAppName().length()>64) {
			return new ReturnT<String>(500, I18nUtil.getString("jobgroup_field_appName_length") );
		}
		//名称不能为空
		if (xxlJobGroup.getTitle()==null || xxlJobGroup.getTitle().trim().length()==0) {
			return new ReturnT<String>(500, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobgroup_field_title")) );
		}

		if (xxlJobGroup.getAddressType()!=0) {//如果注册方式是手动个录入的话，则地址列表不能为空
			if (xxlJobGroup.getAddressList()==null || xxlJobGroup.getAddressList().trim().length()==0) {
				return new ReturnT<String>(500, I18nUtil.getString("jobgroup_field_addressType_limit") );
			}
			//多个地址列表用逗号分割
			String[] addresss = xxlJobGroup.getAddressList().split(",");
			for (String item: addresss) {
				if (item==null || item.trim().length()==0) {
					return new ReturnT<String>(500, I18nUtil.getString("jobgroup_field_registryList_unvalid") );
				}
			}
		}
		//保存执行器
		int ret = xxlJobGroupDao.save(xxlJobGroup);
		return (ret>0)?ReturnT.SUCCESS:ReturnT.FAIL;
	}

	/***
	 * 更新一个执行器
	 * @param xxlJobGroup
	 * @return
	 */
	@RequestMapping("/update")
	@ResponseBody
	public ReturnT<String> update(XxlJobGroup xxlJobGroup){
		// AppName不能为空，且长度是大于3，小于21
		if (xxlJobGroup.getAppName()==null || xxlJobGroup.getAppName().trim().length()==0) {
			return new ReturnT<String>(500, (I18nUtil.getString("system_please_input")+"AppName") );
		}
		if (xxlJobGroup.getAppName().length()<4 || xxlJobGroup.getAppName().length()>64) {
			return new ReturnT<String>(500, I18nUtil.getString("jobgroup_field_appName_length") );
		}
		//名称不能为空
		if (xxlJobGroup.getTitle()==null || xxlJobGroup.getTitle().trim().length()==0) {
			return new ReturnT<String>(500, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobgroup_field_title")) );
		}
		if (xxlJobGroup.getAddressType() == 0) {
			// 0=自动注册 根据appName获得注册上来的执行器的地址列表
			List<String> registryList = findRegistryByAppName(xxlJobGroup.getAppName());
			String addressListStr = null;
			//对注册的地址列表进行排序
			if (registryList!=null && !registryList.isEmpty()) {
				Collections.sort(registryList);
				addressListStr = "";
				for (String item:registryList) {
					addressListStr += item + ",";
				}
				addressListStr = addressListStr.substring(0, addressListStr.length()-1);
			}
			xxlJobGroup.setAddressList(addressListStr);
		} else {
			// 1=手动录入
			if (xxlJobGroup.getAddressList()==null || xxlJobGroup.getAddressList().trim().length()==0) {
				return new ReturnT<String>(500, I18nUtil.getString("jobgroup_field_addressType_limit") );
			}
			String[] addresss = xxlJobGroup.getAddressList().split(",");
			for (String item: addresss) {
				if (item==null || item.trim().length()==0) {
					return new ReturnT<String>(500, I18nUtil.getString("jobgroup_field_registryList_unvalid") );
				}
			}
		}

		int ret = xxlJobGroupDao.update(xxlJobGroup);
		return (ret>0)?ReturnT.SUCCESS:ReturnT.FAIL;
	}

	/****
	 * 根据appName查询注册的执行器的地址
	 * @param appNameParam
	 * @return
	 */
	private List<String> findRegistryByAppName(String appNameParam){
		HashMap<String, List<String>> appAddressMap = new HashMap<String, List<String>>();
		//查询 XxlJobRegistry 表查找，因为当一个执行器注册到调度器上的话，会被保存到数据库表里
		List<XxlJobRegistry> list = XxlJobAdminConfig.getAdminConfig().
				getXxlJobRegistryDao().findAll(RegistryConfig.DEAD_TIMEOUT);
		if (list != null) {
			for (XxlJobRegistry item: list) {
				//判断是否是执行器
				if (RegistryConfig.RegistType.EXECUTOR.name().equals(item.getRegistryGroup())) {
					String appName = item.getRegistryKey();
					List<String> registryList = appAddressMap.get(appName);
					if (registryList == null) {
						registryList = new ArrayList<String>();
					}

					if (!registryList.contains(item.getRegistryValue())) {
						registryList.add(item.getRegistryValue());
					}
					appAddressMap.put(appName, registryList);
				}
			}
		}
		return appAddressMap.get(appNameParam);
	}

	/***
	 * 移除一个执行器
	 * @param id
	 * @return
	 */
	@RequestMapping("/remove")
	@ResponseBody
	public ReturnT<String> remove(int id){

		// valid
		int count = xxlJobInfoDao.pageListCount(0, 10, id, null, null);
		if (count > 0) {
			return new ReturnT<String>(500, I18nUtil.getString("jobgroup_del_limit_0") );
		}

		List<XxlJobGroup> allList = xxlJobGroupDao.findAll();
		if (allList.size() == 1) {
			return new ReturnT<String>(500, I18nUtil.getString("jobgroup_del_limit_1") );
		}

		int ret = xxlJobGroupDao.remove(id);
		return (ret>0)?ReturnT.SUCCESS:ReturnT.FAIL;
	}

}
