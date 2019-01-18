package com.wzb.infa.infaobj.base;

import org.dom4j.Element;

public interface InfaXML {


	
	public Element getSource();
	
	public Element getTarSource();
	
	public Element getTarget();
	
	public Element getMapping();

	public Element getWorkflow();
	
	public String getWorkflowName();
	
	public Element addToFolder(Element folder);

	public void print();

}
