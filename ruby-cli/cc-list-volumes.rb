# include thrift-generated code
$:.push('../thrift-generated-src-ruby', __FILE__)

require 'thrift'
require 'control_center'
require 'pp'

begin
  port = ARGV[0]

  transport = Thrift::FramedTransport.new(Thrift::Socket.new('localhost', port))
  protocol = Thrift::BinaryProtocol.new(transport)
  client = ControlCenter::Client.new(protocol)

  transport.open()

  req = ListVolumesRequest.new()    
  req.accountId = ARGV[1].to_i

  pp req
  result = client.listVolumes(req)
  pp result
  
  transport.close()
rescue
  puts $!
end
