class String
  def sanitized
    self.gsub("'", "''")
  end
end
