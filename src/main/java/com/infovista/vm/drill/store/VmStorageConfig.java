package com.infovista.vm.drill.store;

import org.apache.drill.common.logical.StoragePluginConfigBase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;


@JsonTypeName(VmStorageConfig.NAME)
public class VmStorageConfig extends StoragePluginConfigBase {

	public final static String NAME = "vm";
	
	private final String vistamartServer;
	private final String vm_user;
	private final String vm_password;
	private final int pageSize;
	
	@JsonCreator
	public VmStorageConfig(
			@JsonProperty("vistamartServer") String vmServer, 
			@JsonProperty("vm_user") String user, 
			@JsonProperty("vm_password") String password,
			@JsonProperty("page_size") Integer pageSize
			) {
		this.vistamartServer = vmServer;
		this.vm_user = user;
		this.vm_password = password;
		if(pageSize == null)
			this.pageSize = 1000;
		else
			this.pageSize = pageSize.intValue(); 
		
	}
	@Override
	public boolean equals(Object o) {
		if(this == o)
			return true;
		if(o==null)
			return false;
		if(getClass() != o.getClass())
			return false;
		VmStorageConfig other = (VmStorageConfig)o;
		if(vistamartServer.equals(other.getVistamartServer())&& vm_user.equals(other.vm_user)&& vm_password.equals(other.vm_password)&& pageSize==other.pageSize)
			return true;
		return false;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
	    int result = 1;
	    result = prime * result + ((vistamartServer == null) ? 0 : vistamartServer.hashCode());
	    result = prime * result + vm_user.hashCode();
	    result = prime *result+ pageSize;
	    result = prime * result + vm_password.hashCode();
	    return result;
	}
	public String getVistamartServer() {
		return vistamartServer;
	}
	public String getVm_user() {
		return vm_user;
	}
	public String getVm_password() {
		return vm_password;
	}
	public int getPageSize() {
		return pageSize;
	}

}
