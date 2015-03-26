# coding: utf-8

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'purview/version'

Gem::Specification.new do |gem|
  gem.name = 'purview'
  gem.version = Purview::VERSION
  gem.authors = ['Jonathan W. Zaleski']
  gem.email = ['JonathanZaleski@gmail.com']
  gem.summary = 'A framework designed to simplify data warehousing'
  gem.description = 'Coming soon!'
  gem.homepage = 'https://github.com/jzaleski/purview'
  gem.license = 'MIT'

  gem.files = `git ls-files`.split($/)
  gem.executables = gem.files.grep(%r{^bin/}) { |file| File.basename(file) }
  gem.test_files = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ['lib']

  gem.add_dependency 'pg', '~> 0.18'

  gem.add_development_dependency 'bundler', '~> 1.0'
  gem.add_development_dependency 'pry', '~> 0.10'
  gem.add_development_dependency 'rake', '~> 10.4'
  gem.add_development_dependency 'rspec', '~> 3.2'
end
