package org.gilbre.process;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.logging.Level;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.gilbre.process.MQClient;
import org.compiere.model.MBPGroup;
import org.compiere.model.MBPartner;
import org.compiere.model.MBPartnerLocation;
import org.compiere.model.MLocation;
import org.compiere.model.MLocator;
import org.compiere.model.MOrder;
import org.compiere.model.MPInstance;
import org.compiere.model.MPriceListVersion;
import org.compiere.model.MProduct;
import org.compiere.model.MProductCategory;
import org.compiere.model.MProductPrice;
import org.compiere.model.MStorageOnHand;
import org.compiere.model.MTax;
import org.compiere.model.MTaxCategory;
import org.compiere.model.MUser;
import org.compiere.model.Query;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.WebEnv;

public class Export2MQ extends SvrProcess {

	private int pProductCategoryID;
	private int pAD_Org_ID;
	private int pC_Order_ID;
	private int pBPartnerGroupID;
	private int pPriceListVersionID;
	private String p_IsSelfService = "Y";

	private String p_LinkProducts = ""; // defaults can be overiden by params
	private String p_LinkCustomers = "";
	private String p_LinkUsers = "";
	private String p_LinkURL = "localhost";
	private String p_SendAll = "N";
	private String errmsg = "";
	private int count = 0;
	private String mainMsg = "";
	private int pTaxCategoryID;

	@Override
	protected void prepare() {
		ProcessInfoParameter[] para = getParameter();
		for (int i = 0; i < para.length; i++) {
			String name = para[i].getParameterName();

			if (name.equals("LinkProducts"))
				p_LinkProducts = (String) para[i].getParameter();
			else if (name.equals("LinkCustomers"))
				p_LinkCustomers = (String) para[i].getParameter();
			else if (name.equals("LinkUsers"))
				p_LinkUsers = (String) para[i].getParameter();
			else if (para[i].getParameter() == null)
				;
			else if (name.equals("LinkURL"))
				p_LinkURL = (String) para[i].getParameter();

			else if (name.equals("IsSelfService"))
				p_IsSelfService = (String) para[i].getParameter();

			else if (name.equals("AD_Org_ID"))
				pAD_Org_ID = para[i].getParameterAsInt();

			else if (name.equals("C_Order_ID"))
				pC_Order_ID = para[i].getParameterAsInt();

			else if (name.equals(MProductCategory.COLUMNNAME_M_Product_Category_ID)) {
				pProductCategoryID = para[i].getParameterAsInt();
			} else if (name.equals(MBPartner.COLUMNNAME_C_BP_Group_ID)) {
				pBPartnerGroupID = para[i].getParameterAsInt();
			} else if (name.equals(MPriceListVersion.COLUMNNAME_M_PriceList_Version_ID)) {
				pPriceListVersionID = para[i].getParameterAsInt();
			} else if (name.equals("SendAll")) {
				p_SendAll = (String) para[i].getParameter();
			} else if (name.equals(MTaxCategory.COLUMNNAME_C_TaxCategory_ID)) {
				pTaxCategoryID = para[i].getParameterAsInt();
			} else {
				log.log(Level.SEVERE, "Unknown Parameter: " + name);
			}
		}

	}

	@Override
	protected String doIt() throws Exception {
		int product_updated = 0;
		int user_updated = 0;
		int bpartner_updated = 0;
		String processlastrun = "2000-01-01";
		String WhereClientID = "AD_Client_ID =" + getAD_Client_ID() + " ";
		if (p_LinkProducts != null) {
			// CODE TO BRING ONLY MODIFIED PRODUCTS SINCE THE LAST
			// SYNCHRONIZATION; USES FIXED ID FOR SYNCHRONIZATION PROCESS
			// AD_Process_ID=53271
			processlastrun = "2000-01-01";
			/*
			 * MPInstance PInstance = new
			 * Query(Env.getCtx(),"AD_PInstance","AD_Process_ID=53271 "+
			 * "AND AD_PInstance.isprocessing = 'N' " ,null)
			 * .addJoinClause("INNER JOIN AD_PInstance_Para b ON (AD_PInstance.AD_PInstance_ID = b.AD_PInstance_ID AND b.parametername = 'LinkProducts' AND b.p_string IS NOT NULL)"
			 * )
			 * .addJoinClause("INNER JOIN AD_PInstance_Para c ON (AD_PInstance.AD_PInstance_ID = c.AD_PInstance_ID AND (c.parametername = 'pAD_Org_ID')) AND c.p_number = "
			 * +pAD_Org_ID) .setOrderBy("AD_PInstance.updated DESC") .first();
			 */
			MPInstance PInstance = new Query(Env.getCtx(), "AD_PInstance",
					"AD_Process_ID=1000002 AND AD_PInstance.isprocessing = 'N' ", get_TrxName()).addJoinClause(
							"INNER JOIN (SELECT AD_PInstance_ID FROM AD_PInstance_Para WHERE parametername = 'LinkProducts' AND p_string = '"
									+ p_LinkProducts + "') b ON AD_PInstance.AD_PInstance_ID = b.AD_PInstance_ID")
							.setOrderBy("AD_PInstance.updated DESC").first();

			if (PInstance != null && p_SendAll.compareTo("N") == 0)
				processlastrun = PInstance.get_ValueAsString("updated");
			log.info("My last product process run was " + processlastrun + " and thus i say hoyye");

			List<MProduct> Products = new Query(Env.getCtx(), MProduct.Table_Name,
					"((" + MProduct.COLUMNNAME_Updated + ">='" + processlastrun + "' AND " + WhereClientID + ") OR "
							+ MProduct.COLUMNNAME_M_Product_ID
							+ " IN (SELECT M_Product_ID FROM M_ProductPrice WHERE updated >='" + processlastrun
							+ "' AND " + WhereClientID + ") OR " + MProduct.COLUMNNAME_M_Product_ID
							+ " IN (SELECT M_Product_ID FROM M_StorageOnHand WHERE updated >='" + processlastrun
							+ "' AND " + WhereClientID + "))",
					get_TrxName()).list();

			product_updated = Products.size();
			log.warning("The number of products to be pushed is " + product_updated + " oyeiya");

			if (product_updated != 0 || p_SendAll.compareTo("Y") == 0) {
				// CREATE LIST OF PRODUCTS

				List<MProduct> queryp = null;
				if (pProductCategoryID > 0) {
					// get products within this category
					queryp = new Query(Env.getCtx(), MProduct.Table_Name,
							MProductCategory.COLUMNNAME_M_Product_Category_ID + "=? AND "
									+ MProduct.COLUMNNAME_IsSelfService + "=? AND ((" + MProduct.COLUMNNAME_Updated
									+ ">='" + processlastrun + "' AND " + WhereClientID + ") OR "
									+ MProduct.COLUMNNAME_M_Product_ID
									+ " IN (SELECT M_Product_ID FROM M_ProductPrice WHERE updated >='" + processlastrun
									+ "' AND " + WhereClientID + ") OR " + MProduct.COLUMNNAME_M_Product_ID
									+ " IN (SELECT M_Product_ID FROM M_Storage WHERE updated >='" + processlastrun
									+ "' AND " + WhereClientID + "))",
							get_TrxName()).setParameters(pProductCategoryID, p_IsSelfService).list();
					// MQClient.setParams(p_LinkURL, 61613, p_LinkProducts);
					// errmsg = MQClient.sendMessage(ProductXML(queryp)) ;
					String message = ProductXML(queryp);
					MQClient.setParams(p_LinkURL, 61613, p_LinkProducts);
					errmsg = MQClient.sendMessage(message);
					if (errmsg.equals("")) {
						mainMsg = "Sent to Queue: " + count + " Products .. ";
						count = 0;
					} else {
						return errmsg;
					}
				} else {
					if (p_SendAll.compareTo("Y") == 0)
						processlastrun = "2000-01-01";
					List<MProductCategory> querypcategory = new Query(getCtx(), MProductCategory.Table_Name, null,
							get_TrxName()).list();
					for (MProductCategory pcategory : querypcategory) {
						queryp = new Query(Env.getCtx(), MProduct.Table_Name,
								MProductCategory.COLUMNNAME_M_Product_Category_ID + "=? AND "
										+ MProduct.COLUMNNAME_IsSelfService + "=? AND ((" + MProduct.COLUMNNAME_Updated
										+ ">='" + processlastrun + "' AND " + WhereClientID + ") OR "
										+ MProduct.COLUMNNAME_M_Product_ID
										+ " IN (SELECT M_Product_ID FROM M_ProductPrice WHERE updated >='"
										+ processlastrun + "' AND " + WhereClientID + ") OR "
										+ MProduct.COLUMNNAME_M_Product_ID
										+ " IN (SELECT M_Product_ID FROM M_Storage WHERE updated >='" + processlastrun
										+ "' AND " + WhereClientID + "))",
								get_TrxName()).setParameters(pcategory.getM_Product_Category_ID(), p_IsSelfService)
										.list();
						String message = ProductXML(queryp);
						if (message.compareTo("No Products") != 0) {
							MQClient.setParams(p_LinkURL, 61613, p_LinkProducts);
							errmsg = MQClient.sendMessage(message);
							if (!errmsg.equals("")) {
								return errmsg;
							}
						}
					}
					mainMsg = "Sent to Queue: " + count + " Products .. ";
					count = 0;
				}
			}

		}
		// Customers Queue
		if (p_LinkCustomers != null) {
			//processlastrun = "2000-01-01";
			MPInstance PInstance = new Query(Env.getCtx(), "AD_PInstance",
					"AD_Process_ID=1000002 " + "AND AD_PInstance.AD_PInstance_ID !="
							+ Env.getProcessInfo(getCtx()).getAD_PInstance_ID(),
					get_TrxName())
							.addJoinClause(
									"INNER JOIN AD_PInstance_Para b ON (AD_PInstance.AD_PInstance_ID = b.AD_PInstance_ID AND b.parametername = 'LinkCustomers' AND b.p_string IS NOT NULL)")
							.addJoinClause(
									"INNER JOIN AD_PInstance_Para c ON (AD_PInstance.AD_PInstance_ID = c.AD_PInstance_ID AND (c.parametername = 'pAD_Org_ID')) AND c.p_number = "
											+ pAD_Org_ID)
							.setOrderBy("AD_PInstance.updated DESC").first();
			if (PInstance != null && p_SendAll.compareTo("N") == 0)
				processlastrun = PInstance.get_ValueAsString("updated");
			   log.info("---------------------------------Process last run for business partners is " + processlastrun);
			bpartner_updated = new Query(Env.getCtx(),
					MBPartner.Table_Name, MBPartner.COLUMNNAME_Updated + ">='" + processlastrun + "' AND "
							+ MBPartner.COLUMNNAME_IsActive + " = 'Y'" + " AND " + WhereClientID,
					get_TrxName()).count();
			if (bpartner_updated != 0 || p_SendAll.compareTo("Y") == 0) {
				if (pBPartnerGroupID > 0) {
					List<MBPartner> querybp = new Query(Env.getCtx(), MBPartner.Table_Name,
							MBPartner.COLUMNNAME_C_BP_Group_ID + "=? AND " + MBPartner.COLUMNNAME_IsCustomer + "=? AND "
									+ MBPartner.COLUMNNAME_Updated + ">='" + processlastrun + "' AND " + WhereClientID,
							get_TrxName()).setParameters(pBPartnerGroupID, "Y").list();
					MBPGroup group = new Query(Env.getCtx(), MBPGroup.Table_Name,
							MBPGroup.COLUMNNAME_C_BP_Group_ID + "=?", get_TrxName()).setParameters(pBPartnerGroupID)
									.first();
					String message = CustomerXML(querybp, group);
					MQClient.setParams(p_LinkURL, 61613, p_LinkCustomers);
					errmsg = MQClient.sendMessage(message);
					if (errmsg.equals("")) {
						mainMsg = mainMsg + count + " Customers .. ";
						count = 0;
					} else
						return errmsg;
				} else {
					List<MBPartner> querybp = new Query(Env.getCtx(),
							MBPartner.Table_Name, MBPartner.COLUMNNAME_IsCustomer + "='Y' AND "
									+ MBPartner.COLUMNNAME_Updated + ">='" + processlastrun + "' AND " + WhereClientID,
							get_TrxName()).list();
					String message = CustomerXML(querybp, null);
					MQClient.setParams(p_LinkURL, 61613, p_LinkCustomers);
					errmsg = MQClient.sendMessage(message);
					if (errmsg.equals("")) {
						mainMsg = mainMsg + count + " Customers .. ";
						count = 0;
					} else
						return errmsg;
				}
			}

		}
		if (p_LinkUsers != null) {
			// Get users updated after last sync
			//processlastrun = "2000-01-01";
			MPInstance PInstance = new Query(Env.getCtx(), "AD_PInstance",
					"AD_Process_ID=53271 " + "AND AD_PInstance.AD_PInstance_ID !="
							+ Env.getProcessInfo(getCtx()).getAD_PInstance_ID(),
					get_TrxName())
							.addJoinClause(
									"INNER JOIN AD_PInstance_Para b ON (AD_PInstance.AD_PInstance_ID = b.AD_PInstance_ID AND b.parametername = 'LinkUsers' AND b.p_string IS NOT NULL)")
							.addJoinClause(
									"INNER JOIN AD_PInstance_Para c ON (AD_PInstance.AD_PInstance_ID = c.AD_PInstance_ID AND (c.parametername = 'pAD_Org_ID')) AND c.p_number = "
											+ pAD_Org_ID)
							.setOrderBy("AD_PInstance.updated DESC").first();
			if (PInstance != null && p_SendAll.compareTo("N") == 0)
				processlastrun = PInstance.get_ValueAsString("updated");
			log.info("----------------process last run for users is "+processlastrun+"-----------------------------");
			user_updated = new Query(Env.getCtx(),
					MUser.Table_Name, "(" + MUser.COLUMNNAME_AD_Org_ID + "= " + pAD_Org_ID + ") " + " AND "
							+ MUser.COLUMNNAME_Updated + ">='" + processlastrun + "' AND " + WhereClientID,
					get_TrxName()).count();
			log.info("===========================the no of users updated id"+user_updated+"===========================");
			bpartner_updated = new Query(
					Env.getCtx(), MBPartner.Table_Name, MBPartner.COLUMNNAME_IsSalesRep + "= 'Y' AND "
							+ MBPartner.COLUMNNAME_Updated + ">='" + processlastrun + "' AND " + WhereClientID,
					get_TrxName()).count();
			log.info("==========================the number of business partner"+bpartner_updated+"====================");
			
			if (user_updated != 0 || p_SendAll.compareTo("Y") == 0 || bpartner_updated != 0) {
				// CREAR LISTADO DE USUARIOS
				List<MUser> queryu = new Query(Env.getCtx(), MUser.Table_Name,
						"(AD_User.Updated >= '" + processlastrun + "' OR bp.Updated >= '" + processlastrun + "') AND ( "
								+ MUser.Table_Name + "." + MUser.COLUMNNAME_AD_Org_ID + "= " + pAD_Org_ID
								+ ") AND AD_User." + WhereClientID,
						get_TrxName())
								.addJoinClause(
										" JOIN C_BPartner bp ON AD_User.C_BPartner_ID = bp.C_BPartner_ID AND bp.IsSalesRep = 'Y'")
								.list();
				String message = UserXML(queryu);
				MQClient.setParams(p_LinkURL, 61613, p_LinkUsers);
				errmsg = MQClient.sendMessage(message);
				if (errmsg.equals("")) {
					mainMsg = mainMsg + count + " Users ";
					count = 0;
				} else
					return errmsg;
			}
		}
		if (mainMsg.equals(""))
			return "Incorrect Parameters. No records have been changed since the last synchronization";
		else
			return mainMsg;
	}

	private String ProductXML(List<MProduct> query) {
		// get ProdCategory details i.e. Name
		if (query.size() <= 0) {
			return "No Products";
		}
		MProductCategory prodcat = null;
		try {
			StringWriter res = new StringWriter();
			XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(res);
			writer.writeStartDocument();
			writer.writeStartElement("entityDetail");
			writer.writeStartElement("type");
			writer.writeCharacters("GilbrePOS");
			writer.writeEndElement();

			for (MProduct product : query) {
				// System.out.println(product.getValue()+" -
				// "+product.getName());
				// get product category
				log.warning("This is the product i am sending to the queue " + product.getName() + " ");
				prodcat = new Query(Env.getCtx(), MProductCategory.Table_Name,
						MProductCategory.COLUMNNAME_M_Product_Category_ID + "=?", get_TrxName())
								.setParameters(product.getM_Product_Category_ID()).first();
				// get pricing details from Product's ProductPrice
				int anyPrice = new Query(Env.getCtx(), MProductPrice.Table_Name,
						MProductPrice.COLUMNNAME_M_PriceList_Version_ID + "=? AND "
								+ MProductPrice.COLUMNNAME_M_Product_ID + "=?",
						get_TrxName()).setParameters(pPriceListVersionID, product.getM_Product_ID()).count();
				// above checks if any exist before fetching them. If not,
				// default should be zero.
				if (anyPrice == 0)
					continue;
				MProductPrice price = new Query(Env.getCtx(), MProductPrice.Table_Name,
						MProductPrice.COLUMNNAME_M_PriceList_Version_ID + "=? AND "
								+ MProductPrice.COLUMNNAME_M_Product_ID + "=?",
						get_TrxName()).setParameters(pPriceListVersionID, product.getM_Product_ID()).first();

				List<MLocator> locators = new Query(Env.getCtx(), MLocator.Table_Name,
						MLocator.COLUMNNAME_M_Warehouse_ID
								+ " IN (SELECT M_Warehouse_ID from M_Warehouse WHERE IsInTransit = 'N' AND IsActive = 'Y' AND AD_Client_ID = "
								+ Env.getAD_Client_ID(getCtx()) + " AND AD_Org_ID = " + pAD_Org_ID + ")",
						null).list();

				for (MLocator locator : locators) {
					BigDecimal store = null;
					if (locator.getM_Warehouse().isInTransit() == true)
						store = new Query(Env.getCtx(), MStorageOnHand.Table_Name,
								MStorageOnHand.COLUMNNAME_M_Product_ID + "=? AND "
										+ MStorageOnHand.COLUMNNAME_M_Locator_ID
										+ " IN (SELECT M_Locator_ID FROM M_Locator WHERE M_Warehouse_ID = ?) ",
								get_TrxName()).setParameters(product.getM_Product_ID(), locator.getM_Warehouse_ID())
										.setOrderBy("Created DESC").sum("QtyOnHand");
					else
						store = new Query(Env.getCtx(), MStorageOnHand.Table_Name,
								MStorageOnHand.COLUMNNAME_M_Product_ID + "=? AND "
										+ MStorageOnHand.COLUMNNAME_M_Locator_ID + "=?",
								get_TrxName()).setParameters(product.getM_Product_ID(), locator.getM_Locator_ID())
										.setOrderBy("Created DESC").sum("QtyOnHand");

					// System.out.println(store);
					// get Warehouse Name from Store Locator to match to POS
					// Locator Name
					MLocator loc = new Query(Env.getCtx(), MLocator.Table_Name, MLocator.COLUMNNAME_M_Locator_ID + "=?",
							get_TrxName()).setParameters(locator.getM_Locator_ID()).first();
					count++;
					writer.writeStartElement("detail");

					writer.writeStartElement("DocType");
					writer.writeCharacters(MProduct.Table_Name);
					writer.writeEndElement(); // </DocType>

					// Warehouse Name as POS name
					writer.writeStartElement("POSLocatorName");
					writer.writeCharacters(loc.getWarehouseName());
					writer.writeEndElement();

					// Send Warehouse ID to queue
					writer.writeStartElement("M_Warehouse_ID");
					writer.writeCharacters(Integer.toString(loc.getM_Warehouse_ID()));
					writer.writeEndElement();

					// Send InTransit Check to queue
					writer.writeStartElement("isInTransit");
					if (loc.getM_Warehouse().isInTransit() == true)
						writer.writeCharacters("Y");
					else
						writer.writeCharacters("N");
					writer.writeEndElement();

					writer.writeStartElement("ProductName");
					String auxname = product.getName().replaceAll("%(?![0-9a-fA-F]{2})", "%25");
					auxname = auxname.replaceAll("\\+", "%2B");
					writer.writeCharacters(URLEncoder.encode(auxname, WebEnv.ENCODING));
					writer.writeEndElement();

					// Storage Data - QtyOnHand
					writer.writeStartElement("QtyOnHand");
					if (store == null || !product.isActive())
						writer.writeCharacters("0");
					else
						writer.writeCharacters(store.toString());
					writer.writeEndElement();

					// ProductCategory model
					writer.writeStartElement(MProductCategory.COLUMNNAME_M_Product_Category_ID);
					writer.writeCharacters(Integer.toString(product.getM_Product_Category_ID()));
					writer.writeEndElement();
					writer.writeStartElement("CategoryName");
					auxname = prodcat.getName().replaceAll("%(?![0-9a-fA-F]{2})", "%25");
					auxname = auxname.replaceAll("\\+", "%2B");
					writer.writeCharacters(URLEncoder.encode(auxname, WebEnv.ENCODING));
					writer.writeEndElement();

					writer.writeStartElement(MProduct.COLUMNNAME_M_Product_ID);
					writer.writeCharacters(Integer.toString(product.getM_Product_ID()));
					writer.writeEndElement();

					// Tax model i.e. Name, Percentage? (Rate is complex in AD)

					// Send an specific Tax Category
					MTaxCategory taxCategory = null;
					MTax tax = null;
					if (pTaxCategoryID > 0) {
						taxCategory = new Query(Env.getCtx(), MTaxCategory.Table_Name,
								MTaxCategory.COLUMNNAME_C_TaxCategory_ID + "=?", get_TrxName())
										.setParameters(pTaxCategoryID).first();
						tax = new Query(Env.getCtx(), MTax.Table_Name,
								MTax.COLUMNNAME_C_TaxCategory_ID + "=? and " + MTax.COLUMNNAME_IsDefault + "='Y'",
								get_TrxName()).setParameters(pTaxCategoryID).first();
					} else {
						taxCategory = new Query(Env.getCtx(), MTaxCategory.Table_Name,
								MTaxCategory.COLUMNNAME_C_TaxCategory_ID + "=?", get_TrxName())
										.setParameters(product.getC_TaxCategory().getC_TaxCategory_ID()).first();
						tax = new Query(Env.getCtx(), MTax.Table_Name,
								MTax.COLUMNNAME_C_TaxCategory_ID + "=? and " + MTax.COLUMNNAME_IsDefault + "='Y'",
								get_TrxName()).setParameters(product.getC_TaxCategory().getC_TaxCategory_ID()).first();
					}

					writer.writeStartElement(MTaxCategory.COLUMNNAME_C_TaxCategory_ID);
					writer.writeCharacters(Integer.toString(taxCategory.getC_TaxCategory_ID()));
					writer.writeEndElement();

					writer.writeStartElement("TaxCategoryName");
					auxname = taxCategory.getName().replaceAll("%(?![0-9a-fA-F]{2})", "%25");
					auxname = auxname.replaceAll("\\+", "%2B");
					writer.writeCharacters(URLEncoder.encode(auxname, WebEnv.ENCODING));
					writer.writeEndElement();
					writer.writeStartElement(MTax.COLUMNNAME_C_Tax_ID);
					writer.writeCharacters(Integer.toString(tax.getC_Tax_ID()));
					writer.writeEndElement();

					writer.writeStartElement("TaxRate");
					writer.writeCharacters(tax.getRate().toString());
					writer.writeEndElement();

					writer.writeStartElement("TaxName");
					auxname = tax.getName().replaceAll("%(?![0-9a-fA-F]{2})", "%25");
					auxname = auxname.replaceAll("\\+", "%2B");
					writer.writeCharacters(URLEncoder.encode(auxname, WebEnv.ENCODING));
					writer.writeEndElement();

					writer.writeStartElement(MProduct.COLUMNNAME_UPC);
					if (product.getUPC() != null) {
						auxname = product.getUPC().replaceAll("%(?![0-9a-fA-F]{2})", "%25");
					} else { // if there is no UPC for Barcode reference, then
								// use its PK ID.
						auxname = new Integer(product.get_ID()).toString();
					}
					auxname = auxname.replaceAll("\\+", "%2B");
					writer.writeCharacters(URLEncoder.encode(auxname, WebEnv.ENCODING));
					writer.writeEndElement();

					// Pricing info
					writer.writeStartElement(price.COLUMNNAME_PriceList);
					writer.writeCharacters(price.getPriceList().toString());
					writer.writeEndElement();

					writer.writeStartElement(price.COLUMNNAME_PriceLimit);
					writer.writeCharacters(price.getPriceLimit().toString());
					writer.writeEndElement();

					writer.writeEndElement(); // </detail>
					// if there exist Storage and exist Price
				}
				// Product
			}

			// end of XML creation
			writer.writeEndElement(); // </entityDetail>
			writer.writeEndDocument();
			if (count == 0)
				return "No Products";
			return res.toString();
		} catch (Exception ex) {
			ex.printStackTrace();
			return "XML creation ERROR";
		}
	}

	private String CustomerXML(List<MBPartner> query, MBPGroup group) {
		try {
			StringWriter res = new StringWriter();
			XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(res);
			writer.writeStartDocument();
			writer.writeStartElement("entityDetail");
			writer.writeStartElement("type");
			writer.writeCharacters("gilbrePOS");
			writer.writeEndElement();

			for (MBPartner partner : query) {
				MUser contact = new Query(getCtx(), MUser.Table_Name,
						MUser.COLUMNNAME_C_BPartner_ID + "=" + partner.get_ID(), get_TrxName())
								.setOrderBy(MUser.COLUMNNAME_Created + " DESC").first();
				log.info("-----------------------------the name of this business partner is" + partner.getName()
						+ "----------------------------------");
				count++;

				writer.writeStartElement("detail");

				writer.writeStartElement("DocType");
				writer.writeCharacters(MBPartner.Table_Name);
				writer.writeEndElement();

				writer.writeStartElement("CustomerName");
				writer.writeCharacters(URLEncoder.encode(partner.getName(), WebEnv.ENCODING));
				writer.writeEndElement();

				writer.writeStartElement(MBPartner.COLUMNNAME_Value);
				writer.writeCharacters(partner.getValue());
				writer.writeEndElement();

				writer.writeStartElement(MBPartner.COLUMNNAME_IsActive);
				if (partner.isActive())
					writer.writeCharacters("Y");
				else
					writer.writeCharacters("N");
				writer.writeEndElement();

				writer.writeStartElement(MBPartner.COLUMNNAME_Description);
				if (partner.getDescription() != null)
					writer.writeCharacters(URLEncoder.encode(partner.getDescription(), WebEnv.ENCODING));
				writer.writeEndElement();

				writer.writeStartElement(MBPartner.COLUMNNAME_C_BPartner_ID);
				writer.writeCharacters(Integer.toString(partner.getC_BPartner_ID()));
				writer.writeEndElement();

				writer.writeStartElement("Address1");
				MBPartnerLocation bploc = new Query(getCtx(), MBPartnerLocation.Table_Name,
						MBPartnerLocation.COLUMNNAME_C_BPartner_ID + " = " + partner.getC_BPartner_ID(), get_TrxName())
								.first();
				if (bploc != null) {
					MLocation loc = new MLocation(getCtx(), bploc.getC_Location_ID(), get_TrxName());
					if (loc.getAddress1() != null)
						writer.writeCharacters(URLEncoder.encode(loc.getAddress1(), WebEnv.ENCODING));
				}
				writer.writeEndElement();

				writer.writeStartElement(MBPartner.COLUMNNAME_TotalOpenBalance);
				writer.writeCharacters(partner.getTotalOpenBalance().toString());
				writer.writeEndElement();

				// New Columns
				writer.writeStartElement("SalesRep_ID");
				if (partner.getSalesRep_ID() > 0)
					writer.writeCharacters(Integer.toString(partner.getSalesRep_ID()));
				writer.writeEndElement();

				writer.writeStartElement("SalesRep_Name");
				if (partner.getSalesRep_ID() > 0)
					writer.writeCharacters(URLEncoder.encode(partner.getSalesRep().getName(), WebEnv.ENCODING));
				writer.writeEndElement();

				writer.writeStartElement(MUser.COLUMNNAME_EMail);
				if (contact != null) {
					if (contact.getEMail() != null)
						writer.writeCharacters(URLEncoder.encode(contact.getEMail(), WebEnv.ENCODING));
				}
				writer.writeEndElement();

				writer.writeStartElement(MUser.COLUMNNAME_Phone);
				if (contact != null) {
					if (contact.getPhone() != null)
						writer.writeCharacters(URLEncoder.encode(contact.getPhone(), WebEnv.ENCODING));
				}
				writer.writeEndElement();

				writer.writeStartElement(MUser.COLUMNNAME_Phone2);
				if (contact != null) {
					if (contact.getPhone2() != null)
						writer.writeCharacters(URLEncoder.encode(contact.getPhone2(), WebEnv.ENCODING));
				}
				writer.writeEndElement();

				writer.writeStartElement("BPartnerBirthday");
				if (contact != null) {
					if (contact.getBirthday() != null)
						writer.writeCharacters(contact.getBirthday().toString());
				}
				writer.writeEndElement();

				writer.writeStartElement("BPartnerGroup");
				if (partner.getC_BP_Group_ID() > 0)
					writer.writeCharacters(URLEncoder.encode(partner.getC_BP_Group().getValue(), WebEnv.ENCODING));
				writer.writeEndElement();
				/*
				 * } catch(NullPointerException e){ // }
				 */
				writer.writeEndElement(); // detail
			}

			writer.writeEndElement(); // entityDetail
			writer.writeEndDocument(); // message
			return res.toString();
		} catch (Exception ex) {
			ex.printStackTrace();
			return "XML creation ERROR";
		}
	}

	/**
	 * Send SalesReps
	 */
	private String UserXML(List<MUser> query) {

		try {
			StringWriter res = new StringWriter();
			XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(res);
			writer.writeStartDocument();
			writer.writeStartElement("entityDetail");
			writer.writeStartElement("type");
			writer.writeCharacters("Users");
			writer.writeEndElement();

			for (MUser user : query) {
				log.info("------------------------this is the user who hv changed--------"+user.getName());
				writer.writeStartElement("detail");
				writer.writeStartElement(MUser.COLUMNNAME_AD_User_ID);
				writer.writeCharacters(Integer.toString(user.getAD_User_ID()));
				writer.writeEndElement();

				writer.writeStartElement(MUser.COLUMNNAME_Name);
				writer.writeCharacters(URLEncoder.encode(user.getName(), WebEnv.ENCODING));
				writer.writeEndElement();

				writer.writeStartElement(MUser.COLUMNNAME_IsActive);
				if (user.isActive())
					writer.writeCharacters("Y");
				else
					writer.writeCharacters("N");
				writer.writeEndElement();
				writer.writeEndElement(); // </detail>
				count++;
			}
			// end of XML creation
			writer.writeEndElement(); // </entityDetail>
			writer.writeEndDocument();
			return res.toString();
		} catch (Exception ex) {
			ex.printStackTrace();
			return "XML creation ERROR";
		}
	}

}
