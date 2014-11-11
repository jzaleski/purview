module Purview
  module Loggers
    class Console < Base
      private

      def default_opts
        super.merge(:stream => STDOUT)
      end
    end
  end
end
