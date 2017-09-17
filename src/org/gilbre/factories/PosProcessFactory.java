package org.gilbre.factories;

import org.adempiere.base.IProcessFactory;
import org.compiere.process.ProcessCall;
import org.gilbre.process.Export2MQ;
import org.gilbre.process.ImportMQ2Order;

public class PosProcessFactory  implements IProcessFactory{

	@Override
	public ProcessCall newProcessInstance(String className) {
		// TODO Auto-generated method stub
		if(className.equals("org.gilbre.process.Export2MQ")){
			return new Export2MQ();
			
		}else if(className.equals("org.gilbre.process.ImportMQ2Order")){
			return new ImportMQ2Order();	
			
		}
		return null;
	}

}
