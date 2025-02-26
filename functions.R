library(readr)
library(dplyr)
library(data.table)

## Increase the time limit for downloads
options(timeout = 10000)

## Function to download 2024 World Population Prospects data
download_wpp24 <-
  function(include_projections = F) {
    
    # Function to download and save a file with a warning if it already exists
    download_file <- 
      function(url, destfile) {
        if (file.exists(destfile)) { # Check if the file already exists
          print(paste("The file", destfile, "already exists. Skipping download."))
        } else {
          download.file(url, destfile = destfile, mode = "wb")
        }
      }
    
    # Create data directory if it does not exist
    if (!dir.exists("data")) {
      dir.create("data")
    }
    
    # Historical data ####
    
    # URLs for the data files
    # Historical data (1950-2023)
    url_fertility <- "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Fertility_by_Age1.csv.gz"
    url_female_lifetable <- "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Female_1950-2023.csv.gz"
    url_male_lifetable <- "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Male_1950-2023.csv.gz" 
    url_pop <- "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_PopulationBySingleAgeSex_Medium_1950-2023.csv.gz"
    
    # Read and save as CSV files if not already saved
    if (!file.exists("data/WPP2024_Fertility_by_Age1.csv")) {
      print("Reading Fertility")
      fertility <- fread(url_fertility)
      print("Saving Fertility")
      fwrite(fertility, "data/WPP2024_Fertility_by_Age1.csv", row.names = FALSE)
    } else {
      print("Fertility data already saved as CSV. Skipping.")
    }
    
    if (!file.exists("data/WPP2024_Life_Table_Complete_Medium_Female_1950-2023.csv")) {
      print("Reading Female Life Table")
      mortality_female <- fread(url_female_lifetable)
      print("Saving Female Life Table")
      fwrite(mortality_female, "data/WPP2024_Life_Table_Complete_Medium_Female_1950-2023.csv", row.names = FALSE)
    } else {
      print("Female mortality data already saved as CSV. Skipping.")
    }
    
    if (!file.exists("data/WPP2024_Life_Table_Complete_Medium_Male_1950-2023.csv")) {
      print("Reading Male Life Table")
      mortality_male <- fread(url_male_lifetable)
      print("Saving Male Life Table")
      fwrite(mortality_male, "data/WPP2024_Life_Table_Complete_Medium_Male_1950-2023.csv", row.names = FALSE)
    } else {
      print("Male mortality data already saved as CSV. Skipping.")
    }
    
    if (!file.exists("data/WPP2024_Population1JanuaryBySingleAgeSex_Medium_1950-2023.csv")) {
      print("Reading Population")
      pop <- fread(url_pop)
      print("Saving Population")
      fwrite(pop, "data/WPP2024_Population1JanuaryBySingleAgeSex_Medium_1950-2023.csv", row.names = FALSE)
    } else {
      print("Population data already saved as CSV. Skipping.")
    }
    
    # Projection data ####
    if(include_projections){
      
      # Projections (2024-2100)
      # Fertility: historical already includes projections
      # url_fertility <- "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Fertility_by_Age1.csv.gz" # Same as 'historical
      url_proj_female_lifetable <- "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Female_2024-2100.csv.gz" 
      url_proj_male_lifetable <- "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Male_2024-2100.csv.gz" 
      url_proj_pop <- "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_PopulationBySingleAgeSex_Medium_2024-2100.csv.gz"
      
      # Read and save as CSV files if not already saved
      
      if (!file.exists("data/WPP2024_Life_Table_Complete_Medium_Female_2024-2100.csv")) {
        print("Reading Female Life Table")
        mortality_proj_female <- fread(url_proj_female_lifetable)
        print("Saving Female Life Table")
        fwrite(mortality_proj_female, "data/WPP2024_Life_Table_Complete_Medium_Female_2024-2100.csv", row.names = FALSE)
      } else {
        print("Female mortality projection data already saved as CSV. Skipping.")
      }
      
      if (!file.exists("data/WPP2024_Life_Table_Complete_Medium_Male_2024-2100.csv")) {
        print("Reading Male Life Table")
        mortality_proj_male <- fread(url_proj_male_lifetable)
        print("Saving Male Life Table")
        fwrite(mortality_proj_male, "data/WPP2024_Life_Table_Complete_Medium_Male_2024-2100.csv", row.names = FALSE)
      } else {
        print("Male mortality data already saved as CSV. Skipping.")
      }
      
      if (!file.exists("data/WPP2024_Population1JanuaryBySingleAgeSex_Medium_2024-2100.csv")) {
        print("Reading Population")
        pop_proj <- fread(url_proj_pop)
        print("Saving Population")
        fwrite(pop_proj, "data/WPP2024_Population1JanuaryBySingleAgeSex_Medium_2024-2100.csv", row.names = FALSE)
      } else {
        print("Population data already saved as CSV. Skipping.")
      }
      
    }
    
    # Return a message indicating the process is complete
    return("2024 World Population Prospects data downloaded and saved successfully as CSV files.")
  }


#download_wpp24()

# Function to load and filter UN World Population Prospects data for mortality (px,qx,mx) and fertility (fx)

UNWPP_data <- function(country, start_year, end_year, sex = c("Female", "Male"), 
                       indicators = c("px", "qx", "mx", "fx"), output_format = c("csv", "RData"), 
                       output_file = "UNWPP_output") {
  
  ys <- start_year:end_year
  
  hist <- any(ys <= 2023)
  proj <- any(ys > 2023)
  
  # IF HISTORICAL DATA IS NEEDED ====
  if(hist){
    print("Getting historical data...")
    # Define file paths
    fertility_file <- "data/WPP2024_Fertility_by_Age1.csv"
    male_lifetable_file <- "data/WPP2024_Life_Table_Complete_Medium_Male_1950-2023.csv"
    female_lifetable_file <- "data/WPP2024_Life_Table_Complete_Medium_Female_1950-2023.csv"
    
    # Check if files exist
    if (!file.exists(fertility_file)) stop("Fertility data file not found: ", fertility_file)
    if (!file.exists(male_lifetable_file)) stop("Male life table data file not found: ", male_lifetable_file)
    if (!file.exists(female_lifetable_file)) stop("Female life table data file not found: ", female_lifetable_file)
    
    # Read the life table data
    WPP2024_male_lifetable <- fread(male_lifetable_file) %>% as.data.frame
    WPP2024_female_lifetable <- fread(female_lifetable_file) %>% as.data.frame
    
    # Combine male and female life table data
    WPP2024_lifetable <- bind_rows(WPP2024_female_lifetable, WPP2024_male_lifetable)
    
    # Initialize an empty list to store data frames for each sex
    combined_sex_data <- list()
    
    # Loop over each specified sex
    for (current_sex in sex) {
      # Initialize a list to store selected data for current sex
      selected_data <- list()
      
      # Filter life table data based on specified indicators
      if ("px" %in% indicators) {
        px <- WPP2024_lifetable %>%
          select(Location, Sex, Time, AgeGrpStart, px) %>%
          filter(Location == country, Time >= start_year, Time <= end_year, Sex == current_sex) %>%
          rename(year = Time, age = AgeGrpStart)
        selected_data$px <- px
      }
      
      if ("qx" %in% indicators) {
        qx <- WPP2024_lifetable %>%
          select(Location, Sex, Time, AgeGrpStart, qx) %>%
          filter(Location == country, Time >= start_year, Time <= end_year, Sex == current_sex) %>%
          rename(year = Time, age = AgeGrpStart)
        selected_data$qx <- qx
      }
      
      if ("mx" %in% indicators) {
        mx <- WPP2024_lifetable %>%
          select(Location, Sex, Time, AgeGrpStart, mx) %>%
          filter(Location == country, Time >= start_year, Time <= end_year, Sex == current_sex) %>%
          rename(year = Time, age = AgeGrpStart)
        selected_data$mx <- mx
      }
      
      # Process fertility data if "fx" indicator and sex is Female
      if ("fx" %in% indicators && current_sex == "Female") {
        fertility_data <- fread(fertility_file) %>% as.data.frame
        asfr <- fertility_data %>%
          filter(Time <= 2023) %>% 
          select(Location, Time, AgeGrpStart, ASFR) %>%
          mutate(ASFR = ASFR / 1000, Sex = "Female") %>%  # Add Sex column explicitly
          filter(Location == country, Time >= start_year, Time <= end_year) %>%
          rename(year = Time, age = AgeGrpStart, fx = ASFR) %>% 
          # Expand fert data frame to include values for age outside the 15:49 age range
          complete(age = 0:100, year = unique(year), Sex = unique(Sex), fill = list(fx = 0))
        
        # Join fertility data with other indicators and fill missing values in `fx` with 0
        if ("px" %in% names(selected_data)) {
          asfr <- left_join(selected_data$px, asfr, by = c("Location", "year", "age", "Sex")) %>%
            mutate(fx = replace_na(fx, 0))
        } else {
          asfr <- asfr %>%
            mutate(fx = replace_na(fx, 0))
        }
        
        selected_data$fx <- asfr
      } else if ("fx" %in% indicators && current_sex == "Male") {
        warning("Fertility data (fx) is only available for females. Ignoring fx for males.")
      }
      
      # Combine all selected data for the current sex
      if (length(selected_data) > 0) {
        combined_data_sex <- Reduce(function(x, y) {
          full_join(x, y, by = c("Location", "year", "age", "Sex")) %>%
            mutate(across(ends_with(".x"), ~ coalesce(.x, get(sub(".x$", ".y", cur_column()))))) %>%
            select(-ends_with(".y"))
        }, selected_data)
        
        combined_sex_data[[current_sex]] <- combined_data_sex
      }
    }
    
    # Combine data for all specified sexes into one data frame
    combined_data_hist <- combined_data <- bind_rows(combined_sex_data)
  }
  
  # IF projection DATA IS NEEDED ====
  if(proj){
    print("Getting projection data...")
    # Define file paths
    fertility_file <- "data/WPP2024_Fertility_by_Age1.csv"
    male_lifetable_file <- "data/WPP2024_Life_Table_Complete_Medium_Male_2024-2100.csv"
    female_lifetable_file <- "data/WPP2024_Life_Table_Complete_Medium_Female_2024-2100.csv"
    
    # Check if files exist
    if (!file.exists(fertility_file)) stop("Fertility data file not found: ", fertility_file)
    if (!file.exists(male_lifetable_file)) stop("Male life table data file not found: ", male_lifetable_file)
    if (!file.exists(female_lifetable_file)) stop("Female life table data file not found: ", female_lifetable_file)
    
    # Read the life table data
    WPP2024_male_lifetable <- fread(male_lifetable_file) %>% as.data.frame
    WPP2024_female_lifetable <- fread(female_lifetable_file) %>% as.data.frame
    
    # Combine male and female life table data
    WPP2024_lifetable <- bind_rows(WPP2024_female_lifetable, WPP2024_male_lifetable)
    
    # Initialize an empty list to store data frames for each sex
    combined_sex_data <- list()
    
    # Loop over each specified sex
    for (current_sex in sex) {
      # Initialize a list to store selected data for current sex
      selected_data <- list()
      
      # Filter life table data based on specified indicators
      if ("px" %in% indicators) {
        px <- WPP2024_lifetable %>%
          select(Location, Sex, Time, AgeGrpStart, px) %>%
          filter(Location == country, Time >= start_year, Time <= end_year, Sex == current_sex) %>%
          rename(year = Time, age = AgeGrpStart)
        selected_data$px <- px
      }
      
      if ("qx" %in% indicators) {
        qx <- WPP2024_lifetable %>%
          select(Location, Sex, Time, AgeGrpStart, qx) %>%
          filter(Location == country, Time >= start_year, Time <= end_year, Sex == current_sex) %>%
          rename(year = Time, age = AgeGrpStart)
        selected_data$qx <- qx
      }
      
      if ("mx" %in% indicators) {
        mx <- WPP2024_lifetable %>%
          select(Location, Sex, Time, AgeGrpStart, mx) %>%
          filter(Location == country, Time >= start_year, Time <= end_year, Sex == current_sex) %>%
          rename(year = Time, age = AgeGrpStart)
        selected_data$mx <- mx
      }
      
      # Process fertility data if "fx" indicator and sex is Female
      if ("fx" %in% indicators && current_sex == "Female") {
        fertility_data <- fread(fertility_file) %>% as.data.frame
        asfr <- fertility_data %>%
          filter(Time > 2023, Variant == "Medium") %>% 
          select(Location, Time, AgeGrpStart, ASFR) %>%
          mutate(ASFR = ASFR / 1000, Sex = "Female") %>%  # Add Sex column explicitly
          filter(Location == country, Time >= start_year, Time <= end_year) %>%
          rename(year = Time, age = AgeGrpStart, fx = ASFR) %>% 
          # Expand fert data frame to include values for age outside the 15:49 age range
          complete(age = 0:100, year = unique(year), Sex = unique(Sex), fill = list(fx = 0))
        
        # Join fertility data with other indicators and fill missing values in `fx` with 0
        if ("px" %in% names(selected_data)) {
          asfr <- left_join(selected_data$px, asfr, by = c("Location", "year", "age", "Sex")) %>%
            mutate(fx = replace_na(fx, 0))
        } else {
          asfr <- asfr %>%
            mutate(fx = replace_na(fx, 0))
        }
        
        selected_data$fx <- asfr
      } else if ("fx" %in% indicators && current_sex == "Male") {
        print("Fertility data (fx) is only available for females. Ignoring fx for males.")
      }
      
      # Combine all selected data for the current sex
      if (length(selected_data) > 0) {
        combined_data_sex <- Reduce(function(x, y) {
          full_join(x, y, by = c("Location", "year", "age", "Sex")) %>%
            mutate(across(ends_with(".x"), ~ coalesce(.x, get(sub(".x$", ".y", cur_column()))))) %>%
            select(-ends_with(".y"))
        }, selected_data)
        
        combined_sex_data[[current_sex]] <- combined_data_sex
      }
    }
    
    # Combine data for all specified sexes into one data frame
    combined_data_proj <- combined_data <- bind_rows(combined_sex_data)
  }
  
  # Consolidate
  if(hist & proj){
    combined_data <- bind_rows(combined_data_hist, combined_data_proj)
  } 
  
  combined_data$Location <- NULL
  
  # Save output if specified
  if (output_format == "csv") {
    write.csv(combined_data, file = paste0(output_file, ".csv"), row.names = FALSE)
    message("Data saved as CSV: ", paste0(output_file, ".csv"))
  } else if (output_format == "RData") {
    save(combined_data, file = paste0(output_file, ".RData"))
    message("Data saved as RData: ", paste0(output_file, ".RData"))
  } else {
    stop("Invalid output format. Please specify either 'csv' or 'RData'.")
  }
  
  return(combined_data)
}


# Function to load and filter UN World Population Prospects data for population (N)

UNWPP_pop <-
  function(country_name, start_year, end_year, sex) {
    
    ys <- start_year:end_year
    
    # Read in population data
    WPP2024_pop <- fread("data/WPP2024_Population1JanuaryBySingleAgeSex_Medium_1950-2023.csv") %>% as.data.frame
    
    if(any(ys > 2023)){
      WPP2024_pop_proj <- fread("data/WPP2024_Population1JanuaryBySingleAgeSex_Medium_2024-2100.csv") %>% as.data.frame
      WPP2024_pop <- bind_rows(WPP2024_pop, WPP2024_pop_proj)
    }
    
    # Select the relevant population column based on sex
    if (sex == "Female") {
      wpp <- WPP2024_pop %>% 
        select(age = AgeGrpStart, country = Location, year = Time, pop = PopFemale)
    } else if (sex == "Male") {
      wpp <- WPP2024_pop %>% 
        select(age = AgeGrpStart, country = Location, year = Time, pop = PopMale)
    } else {
      stop("Invalid sex. Please specify 'Male' or 'Female'.") 
    }
    
    # Filter by country and reshape data
    wpp <- wpp %>% 
      filter(country == country_name, year >= start_year, year <= end_year) %>%
      pivot_wider(names_from = year, values_from = pop) %>%
      select(-age, -country) %>%
      as.matrix()
    
    # Add row names for age groups
    row.names(wpp) <- 0:(nrow(wpp) - 1)
    
    return(wpp)
  }
