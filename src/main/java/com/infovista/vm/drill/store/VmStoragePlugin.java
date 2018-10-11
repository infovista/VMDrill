package com.infovista.vm.drill.store;

import static java.util.Collections.singletonList;
import static javax.xml.ws.BindingProvider.ENDPOINT_ADDRESS_PROPERTY;
import static javax.xml.ws.BindingProvider.PASSWORD_PROPERTY;
import static javax.xml.ws.BindingProvider.USERNAME_PROPERTY;
import static javax.xml.ws.handler.MessageContext.HTTP_REQUEST_HEADERS;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.infovista.vistamart.datamodel.ws.v8.DataModelServiceV80;
import com.infovista.vistamart.datamodel.ws.v8.DataModelV80;
import com.infovista.vistamart.datamodel.ws.v8.DisplayRate;
import com.infovista.vistamart.datamodel.ws.v8.MatrixDataInputColumnType;

public class VmStoragePlugin extends AbstractStoragePlugin{
//	private final static Logger logger = LoggerFactory.getLogger(VmStoragePlugin.class);
	private DataModelV80 service = null;
	private VmStorageConfig configuration;
	private List<String> columns = null;
	
	static Map<String,MatrixDataInputColumnType> attributesInTable = new LinkedHashMap<>();
	//static Map<String, MatrixDataInputColumnType> attributesInDataTable = new LinkedHashMap<>();
	public Map<String,VmTableDef> indicatorsByDataTable = new ConcurrentHashMap<>();
	public Map<String,Map<String,PropertyDesc>> propertiesInTables = new ConcurrentHashMap<>();
	static String listOfDispalyRates;
	static public Map<String, DisplayRate> drMapName = new HashMap<String,DisplayRate>();
	static {
		attributesInTable.put(VmTable.NAME_COLUMN_NAME, MatrixDataInputColumnType.INS_NAME);
		attributesInTable.put(VmTable.ID_COLUMN_NAME, MatrixDataInputColumnType.INS_ID);
		attributesInTable.put(VmTable.TAG_COLUMN_NAME, MatrixDataInputColumnType.INS_TAG);
		attributesInTable.put(VmTable.TIMESTAMP_COLUMN_NAME, MatrixDataInputColumnType.TIMESTAMP);
		attributesInTable.put(VmTable.PROXY_OF_COLUMN_NAME, MatrixDataInputColumnType.BASIC_TAG);

		drMapName.put("15s",DisplayRate.SEC_15);
		drMapName.put("1m",DisplayRate.MIN_1);
		drMapName.put("5m",DisplayRate.MIN_5);
		drMapName.put("10m",DisplayRate.MIN_10);
		drMapName.put("15m",DisplayRate.MIN_15);
		drMapName.put("30m",DisplayRate.MIN_30);
		drMapName.put("H",DisplayRate.HOUR);
		drMapName.put("D",DisplayRate.DAY);
		drMapName.put("W",DisplayRate.WEEK);
		drMapName.put("M",DisplayRate.MONTH);
		drMapName.put("Q",DisplayRate.QUARTER);
		drMapName.put("Y",DisplayRate.YEAR);
		listOfDispalyRates = drMapName.keySet().stream().collect(Collectors.joining(", "));
	}
	
	public VmStoragePlugin(VmStorageConfig config,DrillbitContext inContext, String inName) {
		super(inContext, inName);
		this.configuration = config; 	
		this.service = getService();
	}

	@Override
	public StoragePluginConfig getConfig() {
		
		return configuration;
	}
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	public List<String> getColumns(){
		return columns;
	}

	@Override
	public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
		VmSchema schema = new VmSchema(this, getName());
		parent.add(getName(),schema);
		
	}
	 @Override
	  public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
		 return ImmutableSet.of(VmPushFilterIntoScan.FILTER_ON_SCAN, VmPushFilterIntoScan.FILTER_ON_PROJECT);
	 }
	
	public  DataModelV80 getService() {
		if(service != null)
			return service;
		// use WSDL file
		final URL url = DataModelV80.class.getResource("DataModel-v8.wsdl");
		final QName name = new QName(
				"urn:com:infovista:vistamart:datamodel:ws:v8",
				"DataModelService_v8_0");
		final DataModelServiceV80 serviceStub = new DataModelServiceV80(url,
				name);

		final DataModelV80 proxy = serviceStub.getSOAPOverHTTP();

		
		final Map<String, List<String>> httpHeaders = new HashMap<String, List<String>>();
		httpHeaders.put("Accept-Encoding", singletonList("gzip"));

		((BindingProvider) proxy).getRequestContext().put(
				ENDPOINT_ADDRESS_PROPERTY,
				"http://" + configuration.getVistamartServer() + ":11080/DataModelService/v8");

		((BindingProvider) proxy).getRequestContext().put(USERNAME_PROPERTY,
				configuration.getVm_user());
		((BindingProvider) proxy).getRequestContext().put(PASSWORD_PROPERTY,
				configuration.getVm_password());

		((BindingProvider) proxy).getRequestContext().put(HTTP_REQUEST_HEADERS,
				httpHeaders);

		service = proxy;
		return proxy;
	}
	
	
 public VmGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> schemaComumns) throws IOException {
	VmScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<VmScanSpec>() {});
	 return new VmGroupScan(this, userName, scanSpec,null);
 }

}
