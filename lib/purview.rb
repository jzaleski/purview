require 'csv'
require 'date'
require 'net/http'
require 'openssl'
require 'ostruct'
require 'set'
require 'time'
require 'uri'

%w[pg mysql2].each { |gem| begin; require gem; rescue LoadError; end }
abort 'Could not load the `pg` or `mysql2` gem' unless defined?(PG) || defined?(Mysql2)

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
