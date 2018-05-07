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
					<MinScaleDenominator>1200</MinScaleDenominator>
					<MaxScaleDenominator>200000000</MaxScaleDenominator> 
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>ttf://Material Icons#0xe52f</WellKnownName>
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
							<Size>24</Size>
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
					<MinScaleDenominator>1200</MinScaleDenominator>
					<MaxScaleDenominator>200000000</MaxScaleDenominator> 
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>ttf://Material Icons#0xe536</WellKnownName>
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
							<Size>24</Size>
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
					<MinScaleDenominator>1200</MinScaleDenominator>
					<MaxScaleDenominator>200000000</MaxScaleDenominator> 
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>ttf://Material Icons#0xe530</WellKnownName>
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
							<Size>20</Size>
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
					<MinScaleDenominator>1200</MinScaleDenominator>
					<MaxScaleDenominator>200000000</MaxScaleDenominator> 
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>ttf://Material Icons#0xe531</WellKnownName>
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
							<Size>18</Size>
						</Graphic>
					</PointSymbolizer>
				</Rule>

				<Rule>
					<Name>Moped</Name>
					<ogc:Filter>
						<ogc:PropertyIsEqualTo>
							<ogc:PropertyName>vehiclemode</ogc:PropertyName>
							<ogc:Literal>4</ogc:Literal>
						</ogc:PropertyIsEqualTo>
					</ogc:Filter>
					<MinScaleDenominator>1200</MinScaleDenominator>
					<MaxScaleDenominator>200000000</MaxScaleDenominator> 
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>ttf://Material Icons#0xe91B</WellKnownName>
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
							<Size>24</Size>
						</Graphic>
					</PointSymbolizer>
				</Rule>
              <Rule>
					<Name>Train</Name>
					<ogc:Filter>
						<ogc:PropertyIsEqualTo>
							<ogc:PropertyName>vehiclemode</ogc:PropertyName>
							<ogc:Literal>5</ogc:Literal>
						</ogc:PropertyIsEqualTo>
					</ogc:Filter>
					<MinScaleDenominator>1200</MinScaleDenominator>
					<MaxScaleDenominator>200000000</MaxScaleDenominator> 
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>ttf://Material Icons#0xe570</WellKnownName>
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
							<Size>20</Size>
						</Graphic>
					</PointSymbolizer>
				</Rule>
<!-- 1:1200 -->

				<Rule>
					<Name>Bike</Name>
					<ogc:Filter>
						<ogc:PropertyIsEqualTo>
							<ogc:PropertyName>vehiclemode</ogc:PropertyName>
							<ogc:Literal>1</ogc:Literal>
						</ogc:PropertyIsEqualTo>
					</ogc:Filter>
					<MaxScaleDenominator>1200</MaxScaleDenominator> 
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>ttf://Material Icons#0xe52f</WellKnownName>
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
							<Size>32</Size>
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
					<MaxScaleDenominator>1200</MaxScaleDenominator> 
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>ttf://Material Icons#0xe536</WellKnownName>
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
							<Size>30</Size>
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
					<MaxScaleDenominator>1200</MaxScaleDenominator> 
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>ttf://Material Icons#0xe530</WellKnownName>
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
							<Size>28</Size>
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
					<MaxScaleDenominator>1200</MaxScaleDenominator> 
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>ttf://Material Icons#0xe531</WellKnownName>
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
							<Size>28</Size>
						</Graphic>
					</PointSymbolizer>
				</Rule>

				<Rule>
					<Name>Moped</Name>
					<ogc:Filter>
						<ogc:PropertyIsEqualTo>
							<ogc:PropertyName>vehiclemode</ogc:PropertyName>
							<ogc:Literal>4</ogc:Literal>
						</ogc:PropertyIsEqualTo>
					</ogc:Filter>
					<MaxScaleDenominator>1200</MaxScaleDenominator> 
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>ttf://Material Icons#0xe91B</WellKnownName>
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
							<Size>30</Size>
						</Graphic>
					</PointSymbolizer>
				</Rule>
              <Rule>
					<Name>Train</Name>
					<ogc:Filter>
						<ogc:PropertyIsEqualTo>
							<ogc:PropertyName>vehiclemode</ogc:PropertyName>
							<ogc:Literal>5</ogc:Literal>
						</ogc:PropertyIsEqualTo>
					</ogc:Filter>
					<MaxScaleDenominator>1200</MaxScaleDenominator> 
					<!-- like a linesymbolizer but with a fill too -->
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>ttf://Material Icons#0xe570</WellKnownName>
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
							<Size>28</Size>
						</Graphic>
					</PointSymbolizer>
				</Rule>
		    </FeatureTypeStyle>
		</UserStyle>
	</NamedLayer>
</StyledLayerDescriptor>