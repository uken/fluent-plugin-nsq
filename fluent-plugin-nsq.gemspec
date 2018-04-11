$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name          = "fluent-plugin-nsq"
  s.version       = `cat VERSION`
  s.authors       = ["lxfontes", "dterror"]
  s.email         = ["lucas@uken.com", "diogo@uken.com"]
  s.description   = %q{NSQ output plugin for Fluentd}
  s.summary       = %q{output plugin for fluentd}
  s.homepage      = "https://github.com/uken/fluent-plugin-nsq"
  s.license       = 'MIT'

  git_files = `git ls-files`.split($/)

  s.files         = git_files.grep(%r{^(lib|fluent|bin)})
  s.executables   = git_files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  s.test_files    = git_files.grep(%r{^(test|spec|features)/})
  s.require_paths = ["lib"]

  s.add_runtime_dependency 'fluentd', ['~> 0.10', '< 0.14']
  s.add_runtime_dependency 'nsq-ruby', '~> 2.1'
  s.add_development_dependency 'rake', '~> 10'
  s.add_development_dependency("test-unit", ["~> 3.2"])
end
