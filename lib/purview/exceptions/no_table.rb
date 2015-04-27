module Purview
  module Exceptions
    class NoTable < Base
      def message
        'Could not find a table'
      end
    end
  end
end
