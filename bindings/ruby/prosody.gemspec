# frozen_string_literal: true

require_relative "lib/prosody/version"

Gem::Specification.new do |spec|
  spec.name = "prosody"
  spec.version = Prosody::VERSION
  spec.authors = ["Joshua Griffith"]
  spec.email = ["joshua.griffith@realgeeks.com"]

  spec.summary = "High-level Kafka interface"
  spec.required_ruby_version = ">= 2.3.0"

  spec.files = Dir["lib/**/*.rb", "ext/**/*.{rs,toml,lock,rb}"]
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/prosody/extconf.rb"]

  spec.add_dependency "rb_sys", "~> 0.9.97"
  spec.add_development_dependency "rake-compiler", "~> 1.2.0"
end
