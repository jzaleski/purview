source 'https://rubygems.org'

group :development do
  if defined?(JRUBY_VERSION)
    gem 'jdbc-jtds', '~> 1.0'
    gem 'jdbc-mysql', '~> 5.0'
    gem 'jdbc-postgres', '~> 9.0'
    gem 'jdbc-sqlite3', '~> 3.0'
  else
    gem 'mysql2', '~> 0.4'
    gem 'pg', '~> 0.20'
    gem 'sqlite3', '~> 1.0'
    gem 'tiny_tds', '~> 1.0'
  end
end

gemspec
