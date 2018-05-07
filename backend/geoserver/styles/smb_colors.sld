<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0" 
		xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd" 
		xmlns="http://www.opengis.net/sld" 
		xmlns:ogc="http://www.opengis.net/ogc" 
		xmlns:xlink="http://www.w3.org/1999/xlink" 
		xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
		<!-- a named layer is the basic building block of an sld document -->

	<NamedLayer>
		<Name>Datapoints</Name>
		<UserStyle>
		    <!-- they have names, titles and abstracts -->
		  
			<Title>Datapoints for SaveMyBike</Title>
			<!-- FeatureTypeStyles describe how to render different features -->
			<!-- a feature type for points -->

			<FeatureTypeStyle>
				<!--FeatureTypeName>Feature</FeatureTypeName-->
				<Rule>
					<Name>Bike</Name>
					<ogc:Filter>
						<ogc:PropertyIsEqualTo>
							<ogc:PropertyName>vehiclemode</ogc:PropertyName>
							<ogc:Literal>1</ogc:Literal>
						</ogc:PropertyIsEqualTo>
					</ogc:Filter>
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>circle</WellKnownName>
								<Fill>
                                   <CssParameter name="fill">
                                     <ogc:Function name="Interpolate">
                                       <!-- Property to transform -->
                                       <ogc:PropertyName>color</ogc:PropertyName>

                                       <!-- Mapping curve definition pairs (input, output) -->
                                       <ogc:Literal>0</ogc:Literal>
                                       <ogc:Literal>#ff0000</ogc:Literal>


                                       <ogc:Literal>255</ogc:Literal>
                                       <ogc:Literal>#ffff00</ogc:Literal>

                                       <!-- Interpolation method -->
                                       <ogc:Literal>color</ogc:Literal>

                                       <!-- Interpolation mode - defaults to linear -->
                                     </ogc:Function>
                                   </CssParameter>
                                 </Fill>
                              	<Stroke>
                                 <CssParameter name="stroke">#000000</CssParameter>
                                 <CssParameter name="stroke-width">1</CssParameter>
                               </Stroke>
							</Mark>
							<Size>6</Size>
						</Graphic>
					</PointSymbolizer>
				</Rule>
				<Rule>
					<Name>Foot</Name>
					<ogc:Filter>
						<ogc:PropertyIsEqualTo>
							<ogc:PropertyName>vehiclemode</ogc:PropertyName>
							<ogc:Literal>0</ogc:Literal>
						</ogc:PropertyIsEqualTo>
					</ogc:Filter>
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>cross</WellKnownName>
								<Fill>
                                   <CssParameter name="fill">
                                     <ogc:Function name="Interpolate">
                                       <!-- Property to transform -->
                                       <ogc:PropertyName>color</ogc:PropertyName>

                                       <!-- Mapping curve definition pairs (input, output) -->
                                       <ogc:Literal>0</ogc:Literal>
                                       <ogc:Literal>#ff0000</ogc:Literal>


                                       <ogc:Literal>255</ogc:Literal>
                                       <ogc:Literal>#ffff00</ogc:Literal>

                                       <!-- Interpolation method -->
                                       <ogc:Literal>color</ogc:Literal>

                                       <!-- Interpolation mode - defaults to linear -->
                                     </ogc:Function>
                                   </CssParameter>
                                 </Fill>
                              	<Stroke>
                                 <CssParameter name="stroke">#000000</CssParameter>
                                 <CssParameter name="stroke-width">1</CssParameter>
                               </Stroke>
							</Mark>
							<Size>8</Size>
						</Graphic>
					</PointSymbolizer>
				</Rule>
				<Rule>
					<Name>Bus</Name>
					<ogc:Filter>
						<ogc:PropertyIsEqualTo>
							<ogc:PropertyName>vehiclemode</ogc:PropertyName>
							<ogc:Literal>2</ogc:Literal>
						</ogc:PropertyIsEqualTo>
					</ogc:Filter>
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>star</WellKnownName>
								<Fill>
                                   <CssParameter name="fill">
                                     <ogc:Function name="Interpolate">
                                       <!-- Property to transform -->
                                       <ogc:PropertyName>color</ogc:PropertyName>

                                       <!-- Mapping curve definition pairs (input, output) -->
                                       <ogc:Literal>0</ogc:Literal>
                                       <ogc:Literal>#ff0000</ogc:Literal>


                                       <ogc:Literal>255</ogc:Literal>
                                       <ogc:Literal>#ffff00</ogc:Literal>

                                       <!-- Interpolation method -->
                                       <ogc:Literal>color</ogc:Literal>

                                       <!-- Interpolation mode - defaults to linear -->
                                     </ogc:Function>
                                   </CssParameter>
                                 </Fill>
                              	<Stroke>
                                 <CssParameter name="stroke">#000000</CssParameter>
                                 <CssParameter name="stroke-width">1</CssParameter>
                               </Stroke>
							</Mark>
							<Size>8</Size>
						</Graphic>
					</PointSymbolizer>
				</Rule>
				<Rule>
					<Name>Car</Name>
					<ogc:Filter>
						<ogc:PropertyIsEqualTo>
							<ogc:PropertyName>vehiclemode</ogc:PropertyName>
							<ogc:Literal>3</ogc:Literal>
						</ogc:PropertyIsEqualTo>
					</ogc:Filter>
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>square</WellKnownName>
								<Fill>
                                   <CssParameter name="fill">
                                     <ogc:Function name="Interpolate">
                                       <!-- Property to transform -->
                                       <ogc:PropertyName>color</ogc:PropertyName>

                                       <!-- Mapping curve definition pairs (input, output) -->
                                       <ogc:Literal>0</ogc:Literal>
                                       <ogc:Literal>#ff0000</ogc:Literal>


                                       <ogc:Literal>255</ogc:Literal>
                                       <ogc:Literal>#ffff00</ogc:Literal>

                                       <!-- Interpolation method -->
                                       <ogc:Literal>color</ogc:Literal>

                                       <!-- Interpolation mode - defaults to linear -->
                                     </ogc:Function>
                                   </CssParameter>
                                 </Fill>
                              	<Stroke>
                                 <CssParameter name="stroke">#000000</CssParameter>
                                 <CssParameter name="stroke-width">1</CssParameter>
                               </Stroke>
							</Mark>
							<Size>6</Size>
						</Graphic>
					</PointSymbolizer>
				</Rule>

		    </FeatureTypeStyle>
		</UserStyle>
	</NamedLayer>
</StyledLayerDescriptor>
