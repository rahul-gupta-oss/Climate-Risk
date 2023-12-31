import pandas as pd





column_headers = ['2022_FR_10',	'2023_FR_10',	'2024_FR_10',	'2025_FR_10',	'2026_FR_10',	'2027_FR_10',	'2028_FR_10',	'2029_FR_10',	'2030_FR_10',	'2031_FR_10',	'2032_FR_10',	'2033_FR_10',	'2034_FR_10',	'2035_FR_10',	'2036_FR_10',	'2037_FR_10',	'2038_FR_10',	'2039_FR_10',	'2040_FR_10',	'2041_FR_10',	'2042_FR_10',	'2043_FR_10',	'2044_FR_10',	'2045_FR_10',	'2046_FR_10',	'2047_FR_10',	'2048_FR_10',	'2049_FR_10',	'2050_FR_10',	'2051_FR_10',	'2052_FR_10',	'2053_FR_10',	'2054_FR_10',	'2055_FR_10',	'2056_FR_10',	'2057_FR_10',	'2058_FR_10',	'2059_FR_10',	'2060_FR_10',	'Area',	'2022_FR_50',	'2023_FR_50',	'2024_FR_50',	'2025_FR_50',	'2026_FR_50',	'2027_FR_50',	'2028_FR_50',	'2029_FR_50',	'2030_FR_50',	'2031_FR_50',	'2032_FR_50',	'2033_FR_50',	'2034_FR_50',	'2035_FR_50',	'2036_FR_50',	'2037_FR_50',	'2038_FR_50',	'2039_FR_50',	'2040_FR_50',	'2041_FR_50',	'2042_FR_50',	'2043_FR_50',	'2044_FR_50',	'2045_FR_50',	'2046_FR_50',	'2047_FR_50',	'2048_FR_50',	'2049_FR_50',	'2050_FR_50',	'2051_FR_50',	'2052_FR_50',	'2053_FR_50',	'2054_FR_50',	'2055_FR_50',	'2056_FR_50',	'2057_FR_50',	'2058_FR_50',	'2059_FR_50',	'2060_FR_50',	'2022_FR_90',	'2023_FR_90',	'2024_FR_90',	'2025_FR_90',	'2026_FR_90',	'2027_FR_90',	'2028_FR_90',	'2029_FR_90',	'2030_FR_90',	'2031_FR_90',	'2032_FR_90',	'2033_FR_90',	'2034_FR_90',	'2035_FR_90',	'2036_FR_90',	'2037_FR_90',	'2038_FR_90',	'2039_FR_90',	'2040_FR_90',	'2041_FR_90',	'2042_FR_90',	'2043_FR_90',	'2044_FR_90',	'2045_FR_90',	'2046_FR_90',	'2047_FR_90',	'2048_FR_90',	'2049_FR_90',	'2050_FR_90',	'2051_FR_90',	'2052_FR_90',	'2053_FR_90',	'2054_FR_90',	'2055_FR_90',	'2056_FR_90',	'2057_FR_90',	'2058_FR_90',	'2059_FR_90',	'2060_FR_90',]  # Replace with your actual column names

# Create an empty DataFrame with only column headers
header_df = pd.DataFrame(columns=column_headers)

# Export the DataFrame to a CSV file
csv_output_path = '/Users/rahulrahul/Documents/Python/Data/Forecasted_Rainfall_output.csv'
header_df.to_csv(csv_output_path, index=False)

print("DataFrame with column headers exported to CSV.")
