require 'purview/raw_consumers/base'

if defined?(JRUBY_VERSION)
  require 'purview/raw_consumers/jruby/base'

  require 'purview/raw_consumers/jruby/kafka'
  require 'purview/raw_consumers/jruby/march_hare'
else
  require 'purview/raw_consumers/bunny'
  require 'purview/raw_consumers/kafka'
end
