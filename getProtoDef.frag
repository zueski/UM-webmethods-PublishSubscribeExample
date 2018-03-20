// input String nsname 
// output String proto

	public static final void getProtoDef(IData pipeline) throws ServiceException
	{
		IDataCursor idc = pipeline.getCursor();
		java.lang.String nsname = idc.first("nsname") ? (java.lang.String) idc.getValue() : null;
		if(nsname != null)
		{
			try
			{
				com.wm.lang.ns.NSNode node = com.wm.app.b2b.server.ns.Namespace.current().getNode(nsname);
				com.wm.lang.ns.NSRecord rec = (com.wm.lang.ns.NSRecord) node;
				java.lang.String protobufString = com.wm.util.coder.ProtobufUtil.doctypeToProtobuf(rec);
				// add package and class settings to make usage easier
				protobufString = "option java_package = \"com.amway.canonical\";\noption java_outer_classname = \"EventUDM\";\n\n" + protobufString;
				idc.insertAfter("proto", protobufString); 
			} catch(Exception e) {
				idc.insertAfter("exception", e);
			}
		}
		idc.destroy();
	}