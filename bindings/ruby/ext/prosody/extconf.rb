require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("prosody/prosody") do |r|
    r.ext_dir = ".../.."
end
