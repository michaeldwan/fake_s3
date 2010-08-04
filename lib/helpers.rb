module FakeS3
  module Helpers
    def sha(thing)
      Digest::SHA1.hexdigest(thing.to_s)
    end
  end
end