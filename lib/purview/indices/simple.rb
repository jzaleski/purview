module Purview
  module Indices
    class Simple < Base
      def initialize(column, opts={})
        super([column], opts)
      end
    end
  end
end
