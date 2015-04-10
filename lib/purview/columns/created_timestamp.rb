module Purview
  module Columns
    class CreatedTimestamp < Timestamp
      private

      def default_opts
        super.merge(:nullable => false)
      end
    end
  end
end
