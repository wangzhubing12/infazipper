package com.wzb.infa.obj;

import org.dom4j.Element;

public interface InfaXML {

	public String getWorkflowName();

	public Element addToFolder(Element folder);

	public void print();

}
