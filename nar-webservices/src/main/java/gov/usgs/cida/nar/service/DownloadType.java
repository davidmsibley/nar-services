package gov.usgs.cida.nar.service;

import java.util.Arrays;
import java.util.List;

public enum DownloadType {
	annualLoad("Annual loads", 
			Arrays.asList(
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_concentration_flow_weighted/COMP",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_concentration_flow_weighted/DAILY",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_concentration_flow_weighted/REG",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_concentration_flow_weighted/REGHIST",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_concentration_flow_weighted/REG_2",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_concentration_flow_weighted/REG_3",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_concentration_flow_weighted/REG_4",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_concentration_flow_weighted/REG_PRELIM",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass/COMP",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass/DAILY",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass/REG",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass/REGHIST",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass/REG_2",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass/REG_3",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass/REG_4",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass/REG_PRELIM",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_lower_95/COMP",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_lower_95/DAILY",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_lower_95/REG",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_lower_95/REGHIST",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_lower_95/REG_2",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_lower_95/REG_3",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_lower_95/REG_4",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_lower_95/REG_PRELIM",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_upper_95/COMP",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_upper_95/DAILY",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_upper_95/REG",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_upper_95/REGHIST",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_upper_95/REG_2",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_upper_95/REG_3",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_upper_95/REG_4",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_mass_upper_95/REG_PRELIM",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_yield/COMP",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_yield/DAILY",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_yield/REG",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_yield/REGHIST",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_yield/REG_2",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_yield/REG_3",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_yield/REG_4",
                                        "http://cida.usgs.gov/def/NAR/procedure/annual_yield/REG_PRELIM"
                    )),
	annualFlow("Annual flow", 
			Arrays.asList(
					"http://cida.usgs.gov/def/NAR/procedure/annual_flow"
					)),
	mayLoad("May loads", //monthly_concentration_flow_weighted
			Arrays.asList(
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass/COMP",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass/DAILY",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass/REG",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass/REGHIST",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass/REG_2",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass/REG_3",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass/REG_4",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass/REG_PRELIM",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_lower_95/COMP",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_lower_95/DAILY",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_lower_95/REG",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_lower_95/REGHIST",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_lower_95/REG_2",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_lower_95/REG_3",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_lower_95/REG_4",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_lower_95/REG_PRELIM",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_upper_95/COMP",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_upper_95/DAILY",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_upper_95/REG",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_upper_95/REGHIST",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_upper_95/REG_2",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_upper_95/REG_3",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_upper_95/REG_4",
                                        "http://cida.usgs.gov/def/NAR/procedure/monthly_mass_upper_95/REG_PRELIM"
                    )),
	mayFlow("Monthly flow", 
			Arrays.asList(
					"http://cida.usgs.gov/def/NAR/procedure/monthly_flow"
					)),
	dailyFlow("Daily flow", 
			Arrays.asList(
					"http://cida.usgs.gov/def/NAR/procedure/daily_flow"
					)),
	sampleConcentrations("Sample Concentrations", 
			Arrays.asList(
					"http://cida.usgs.gov/def/NAR/procedure/discrete_concentration"
					)),
	siteAttribute("Site attribute", 
			null 
			);

	private String title;
	private List<String> procedures;
	DownloadType(String title, List<String> procedures) {
		this.title = title;
		this.procedures = procedures;
	}

	public String getTitle() {
		return title;
	}
	
	public List<String> getProcedures() {
		return procedures;
	}
	
		
	public static String getModTypeFromProcedure(String procedure) {
		return procedure.substring(procedure.lastIndexOf("/") + 1);
	}
	
	public static String getColumnNameFromProcedure(String procedure) {
		String[] components = procedure.split("/");
		return components[components.length-2];
	}
}
