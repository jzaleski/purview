source 'https://rubygems.org'

group :development do
  if defined?(JRUBY_VERSION)
    gem 'jdbc-jtds', '~> 1.3'
    gem 'jdbc-mysql', '~> 5.1'
    gem 'jdbc-postgres', '~> 9.4'
    gem 'jdbc-sqlite3', '~> 3.15'

    gem 'jruby-kafka', '~> 4.2'
    gem 'march_hare', '~> 3.0'
  else
    gem 'mysql2', '~> 0.4'
    gem 'pg', '~> 0.20'
    gem 'sqlite3', '~> 1.3'
    gem 'tiny_tds', '~> 1.3'

    gem 'bunny', '~> 2.7'
    gem 'ruby-kafka', '~> 0.3'
  end
end

gemspec
