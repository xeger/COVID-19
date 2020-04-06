#! /usr/bin/env ruby

require 'csv'
require 'pry'
require 'set'
require 'time'

c = '/Users/tony/Code/CSSEGISandData/COVID-19'
INPUT_FILES = '#{INPUT_REPO}/csse_covid_19_data/csse_covid_19_daily_reports/*.csv'

INTERESTING_ADM0S = ['Iceland', 'Italy', 'UK', 'US', 'Brazil', 'Germany', 'France']

ADM0_ALIASES = {
  'United Kingdom' => 'UK'
}

ADM1_NAMES = {
  "Alaska"=>"AK",
  "Alabama"=>"AL",
  "Arkansas"=>"AR",
  "American Samoa"=>"AS",
  "Arizona"=>"AZ",
  "California"=>"CA",
  "Colorado"=>"CO",
  "Connecticut"=>"CT",
  "District of Columbia"=>"DC",
  "Delaware"=>"DE",
  "Florida"=>"FL",
  "Georgia"=>"GA",
  "Guam"=>"GU",
  "Hawaii"=>"HI",
  "Iowa"=>"IA",
  "Idaho"=>"ID",
  "Illinois"=>"IL",
  "Indiana"=>"IN",
  "Kansas"=>"KS",
  "Kentucky"=>"KY",
  "Louisiana"=>"LA",
  "Massachusetts"=>"MA",
  "Maryland"=>"MD",
  "Maine"=>"ME",
  "Michigan"=>"MI",
  "Minnesota"=>"MN",
  "Missouri"=>"MO",
  "Mississippi"=>"MS",
  "Montana"=>"MT",
  "North Carolina"=>"NC",
  "North Dakota"=>"ND",
  "Nebraska"=>"NE",
  "New Hampshire"=>"NH",
  "New Jersey"=>"NJ",
  "New Mexico"=>"NM",
  "Nevada"=>"NV",
  "New York"=>"NY",
  "Ohio"=>"OH",
  "Oklahoma"=>"OK",
  "Oregon"=>"OR",
  "Pennsylvania"=>"PA",
  "Puerto Rico"=>"PR",
  "Rhode Island"=>"RI",
  "South Carolina"=>"SC",
  "South Dakota"=>"SD",
  "Tennessee"=>"TN",
  "Texas"=>"TX",
  "Utah"=>"UT",
  "Virginia"=>"VA",
  "Virgin Islands"=>"VI",
  "Vermont"=>"VT",
  "Washington"=>"WA",
  "Wisconsin"=>"WI",
  "West Virginia"=>"WV",
  "Wyoming"=>"WY"
}
ADM1_CODES = ADM1_NAMES.invert

# US Census county list has some odd spellings but we consider it canonical.
# Map CSSE/NWS names to census names for some exceptional cases.
ADM2_ALIASES = {
  'Baltimore City' => 'Baltimore city', # MD
  'Desoto' => 'DeSoto', # MS, FL
  'Fairfax City' => 'Fairfax', # VA
  'Franklin City' => 'Franklin', # VA
  'Kansas City' => 'Jackson', # MO (tri-county city, mostly in Jackson however)
  'LeSeur' => 'Le Sueur', # MN
  'New York City' => 'New York',
  'Richmond City' => 'Richmond', # VA
  'Roanoke City' => 'Roanoke', # VA
  'St. Louis City' => 'St Louis city', # MO
}

UNASSIGNED = /^(unassigned|unknown|out[- ]of)/i

def hashify(csv)
  header = nil
  rows = []
  csv.each do |row|
    if header.nil?
      header = row
    else
      rows << row
    end
  end

  rows.map do |r|
    i = 0;
    r.inject({}) do |h, v|
      h[header[i]] = v
      i += 1
      h
    end
  end
end

def unalias_adm0(str)
  ADM0_ALIASES[str] || str
end

def encode_adm1(str)
  str = str.split(/[, ]+/).last if str =~ /,/
  ADM1_NAMES[str] || (ADM1_CODES[str] && str)
end

def decode_adm1(str)
  str = str.split(/[, ]+/).last if str =~ /,/
  ADM1_CODES[str] || (ADM1_NAMES[str] && str)
end

def unalias_adm2(str)
  str = ADM2_ALIASES[str] || str
  str = str&.gsub(/[.']/, '')
  str&.gsub!(/ county$/i, '')
  str
end

# Output: wide table (for spreadsheet)
def time_series_confirmed_by_adm0
  by_date_adm0 = {}

  Dir.glob(INPUT_FILES).each do  |file|
    month, day, year = File.basename(file, '.csv').split('-').map(&:to_i)
    date = Date.new(year,month,day).strftime('%Y-%m-%d')
    hashify(CSV.new(File.read(file))).each do |row|
      adm0 = unalias_adm0(row['Country/Region'] || row['Country_Region'])
      next unless INTERESTING_ADM0S.include? adm0
      by_date_adm0[date] ||= Hash.new(0)
      by_date_adm0[date][adm0] += Integer(row['Confirmed'] || 0)
    end
  end

  puts (['Date'] + INTERESTING_ADM0S).join ","

  by_date_adm0.keys.sort.each do |date|
    adm0s = by_date_adm0[date]
    counts = INTERESTING_ADM0S.map { |adm0| adm0s[adm0] }
    puts %Q{#{date},#{counts.join(",")}}
  end
end

# Output: wide table (for spreadsheet)
def time_series_confirmed_by_adm1(for_adm0='US')
  by_date_adm1 = {}

  Dir.glob(INPUT_FILES).each do  |file|
    month, day, year = File.basename(file, '.csv').split('-').map(&:to_i)
    date = Date.new(year,month,day).strftime('%Y-%m-%d')
    hashify(CSV.new(File.read(file))).each do |row|
      adm0 = row['Country/Region'] || row['Country_Region']
      next unless adm0 == for_adm0
      adm1 = encode_adm1(row['Province/State'] || row['Province_State'])
      next unless adm1
      by_date_adm1[date] ||= Hash.new(0)
      by_date_adm1[date][adm1] += Integer(row['Confirmed'] || 0)
    end
  end

  all_adm1s = by_date_adm1.values.map { |h| Set.new(h.keys) }.reduce(:union).to_a.sort

  puts (['Date'] + all_adm1s).join ","

  by_date_adm1.keys.sort.each do |date|
    by_adm1 = by_date_adm1[date]
    counts = all_adm1s.map { |adm1| by_adm1[adm1] }
    puts %Q{#{date},#{counts.join(",")}}
  end
end

# Output: narrow table, no header row (for import to DB)
def time_series_confirmed_by_adm2(for_adm0='US', for_date=Date.today)
  by_date_locale = {}

  Dir.glob(INPUT_FILES).each do  |file|
    month, day, year = File.basename(file, '.csv').split('-').map(&:to_i)
    date = Date.new(year,month,day)
    next if for_date && date != for_date
    date = date.strftime('%Y-%m-%d')
    hashify(CSV.new(File.read(file))).each do |row|
      adm0 = row['Country/Region'] || row['Country_Region']
      next unless adm0 == for_adm0
      adm1 = decode_adm1(row['Province/State'] || row['Province_State'])
      adm2 = unalias_adm2(row['Admin2'])
      next unless adm1 && adm2 && adm2 !~ UNASSIGNED
      locale = [adm2, adm1].join ','
      by_date_locale[date] ||= Hash.new(0)
      by_date_locale[date][locale] += Integer(row['Confirmed'] || 0)
    end
  end

  by_date_locale.each_pair do |date, by_locale|
    by_locale.each_pair do |locale, confirmed|
      adm2, adm1 = locale.split(',')
      puts [date, %Q{"#{adm2}"}, %Q{"#{adm1}"}, confirmed].join(',')
    end
  end
end

def freshen_input_data
  Dir.chdir(INPUT_REPO) do
    system('git pull origin master') || raise 'Failed to update input data'
  end
end

freshen_input_data

# INITIAL CATCH UP
# (Date.new(2020,3,22)..Date.new(2020,4,3)).each do |date|
#   time_series_confirmed_by_adm2('US', date)
# end

adm0, date = ARGV
date = Date.parse(date) rescue nil

case ARGV.first
when 'US' then time_series_confirmed_by_adm2(adm0, date)
else time_series_confirmed_by_adm0()
end
