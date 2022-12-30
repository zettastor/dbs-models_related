# include thrift-generated code
$:.push('../thrift-generated-src-ruby', __FILE__)

require 'thrift'
require 'information_center'
require 'pp'

begin
  port = ARGV[0]

  transport = Thrift::FramedTransport.new(Thrift::Socket.new('localhost', port))
  protocol = Thrift::BinaryProtocol.new(transport)
  client = InformationCenter::Client.new(protocol)

  transport.open()

  req = AuthenticateAccountRequest.new()    
  req.accountName = ARGV[1]
  req.password = ARGV[2]

  pp req
  result = client.authenticateAccount(req)
  pp result
  
  transport.close()
rescue
  puts $!
end
