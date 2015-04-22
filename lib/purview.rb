require 'csv'
require 'date'
require 'net/http'
require 'openssl'
require 'ostruct'
require 'set'
require 'time'
require 'uri'

%w[mysql2 pg].each { |gem| begin; require gem; rescue LoadError; end }
abort 'Could not load the `mysql2` or `pg` gem' unless defined?(Mysql2) || defined?(PG)

require 'purview/mixins'
require 'purview/refinements'

require 'purview/columns'
require 'purview/connections'
require 'purview/databases'
require 'purview/exceptions'
require 'purview/loaders'
require 'purview/loggers'
require 'purview/parsers'
require 'purview/pullers'
require 'purview/structs'
require 'purview/tables'
require 'purview/types'
require 'purview/version'
