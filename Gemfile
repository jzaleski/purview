source 'https://rubygems.org'

group :development do
  if defined?(JRUBY_VERSION)
    gem 'jdbc-jtds', '~> 1.3'
    gem 'jdbc-mysql', '~> 5.1'
    gem 'jdbc-postgres', '~> 9.4'
  else
    gem 'mysql2', '~> 0.3'
    gem 'pg', '~> 0.18'
    gem 'tiny_tds', '~> 0.6'
  end
end

gemspec
