package com.wzb.infa.obj;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import com.wzb.infa.dbutils.DbUtil;
import com.wzb.infa.properties.InfaProperty;

public abstract class BaseInfaXML implements InfaXML {

	protected Element source;
	protected Element tarSource;
	protected Element target;
	protected Element mapping;
	protected Element workflow;
	private static final Logger logger = Logger.getLogger(BaseInfaXML.class);

	@Override
	public String getWorkflowName() {
		return workflow.attributeValue("NAME");
	}

	@Override
	public Element addToFolder(Element folder) {
		folder.add(this.getSource());
		if (this.getTarSource() != null)
			folder.add(this.getTarSource());
		folder.add(this.getTarget());
		folder.add(this.getMapping());
		folder.add(this.getWorkflow());
		return folder;
	}

	@Override
	public void print() {
		OutputFormat format = new OutputFormat("    ", true);
		XMLWriter xmlWriter;
		try {
			xmlWriter = new XMLWriter(format);
			xmlWriter.write(this.getSource());
			if (this.getTarSource() != null)
				xmlWriter.write(this.getTarSource());
			xmlWriter.write(this.getTarget());
			xmlWriter.write(this.getMapping());
			xmlWriter.write(this.getWorkflow());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public Element getSource() {
		return source;
	}

	public void setSource(Element source) {
		this.source = source;
	}

	@Override
	public Element getTarSource() {
		return tarSource;
	}

	public void setTarSource(Element tarSource) {
		this.tarSource = tarSource;
	}

	@Override
	public Element getTarget() {
		return target;
	}

	public void setTarget(Element target) {
		this.target = target;
	}

	@Override
	public Element getMapping() {
		return mapping;
	}

	public void setMapping(Element mapping) {
		this.mapping = mapping;
	}

	@Override
	public Element getWorkflow() {
		return workflow;
	}

	public void setWorkflow(Element workflow) {
		this.workflow = workflow;
	}

	public String getTargetName(String owner, String tableName) {
		String targetName = null;
		InfaProperty infaProperty = InfaProperty.getInstance();
		String rule = infaProperty.getProperty("target.name.rule", "default") + tableName;
		if ("default".equals(rule)) {
			targetName = infaProperty.getProperty("target.prefix", "") + tableName;
			if (targetName.length() > 30) {
				targetName = targetName.substring(0, 30);
			}
		} else if ("database".equals(rule)) {
			try {
				targetName = DbUtil.getInstance().getTargetTable(owner, tableName);
			} catch (SQLException e) {
				targetName = infaProperty.getProperty("target.prefix", "") + tableName;
				if (targetName.length() > 30) {
					targetName = targetName.substring(0, 30);
				}
			}
		}

		return targetName;
	}
}
