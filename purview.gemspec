# coding: utf-8

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'purview/version'

Gem::Specification.new do |gem|
  gem.name = 'purview'
  gem.version = Purview::VERSION
  gem.authors = ['Jonathan W. Zaleski']
  gem.email = ['JonathanZaleski@gmail.com']
  gem.summary = 'A framework created to simplify data-warehousing'
  gem.description = 'An easy to use configuration-driven framework created to simplify data-warehousing'
  gem.homepage = 'https://github.com/jzaleski/purview'
  gem.license = 'MIT'

  gem.files = `git ls-files`.split($/)
  gem.executables = gem.files.grep(%r{^bin/}) { |file| File.basename(file) }
  gem.test_files = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ['lib']

  gem.required_ruby_version = '>= 2.1.0'

  gem.requirements << '`mysql2` or `jdbc-mysql` gem'
  gem.requirements << '`pg` or `jdbc-postgres` gem'
  gem.requirements << '`sqlite3` or `jdbc-sqlite3` gem'
  gem.requirements << '`tiny_tds` or `jdbc-jtds` gem'

  gem.requirements << '`bunny` or `march_hare` gem (if consuming from RabbitMQ)'
  gem.requirements << '`ruby-kafka` or `jruby-kafka` gem (if consuming from Kafka)'

  gem.add_development_dependency 'pry', '~> 0.10'
  gem.add_development_dependency 'rake', '~> 12.0'
  gem.add_development_dependency 'rb-readline', '~> 0.5'
  gem.add_development_dependency 'rspec', '~> 3.6'
end
