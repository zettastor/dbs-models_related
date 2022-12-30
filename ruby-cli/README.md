    Hongqi-Wangs-iMac:ruby-cli wanghq$ ruby sample.rb
    /Users/wanghq/workspace/java/c5/pengyun-main/pengyun-datanode_model/thrift-generated-src-ruby/controlcenter_types.rb:59: warning: already initialized constant REQUESTID
    /Users/wanghq/workspace/java/c5/pengyun-main/pengyun-datanode_model/thrift-generated-src-ruby/controlcenter_types.rb:60: warning: already initialized constant VOLUMEID
    /Users/wanghq/workspace/java/c5/pengyun-main/pengyun-datanode_model/thrift-generated-src-ruby/controlcenter_types.rb:61: warning: already initialized constant ACCOUNTID
    /Users/wanghq/workspace/java/c5/pengyun-main/pengyun-datanode_model/thrift-generated-src-ruby/controlcenter_types.rb:63: warning: already initialized constant FIELDS
    /Users/wanghq/workspace/java/c5/pengyun-main/pengyun-datanode_model/thrift-generated-src-ruby/controlcenter_types.rb:79: warning: already initialized constant REQUESTID
    /Users/wanghq/workspace/java/c5/pengyun-main/pengyun-datanode_model/thrift-generated-src-ruby/controlcenter_types.rb:80: warning: already initialized constant VOLUMEMETADATA
    /Users/wanghq/workspace/java/c5/pengyun-main/pengyun-datanode_model/thrift-generated-src-ruby/controlcenter_types.rb:81: warning: already initialized constant SEGMENTSMETADATA
    /Users/wanghq/workspace/java/c5/pengyun-main/pengyun-datanode_model/thrift-generated-src-ruby/controlcenter_types.rb:83: warning: already initialized constant FIELDS
    ********************Creating super admin account
    <CreateAccountRequest accountName:"super-admin-account1388526489", password:"123456", accountType:SuperAdmin (1)>
    <CreateAccountResponse accountId:1998661244848372247, accountName:"super-admin-account1388526489">
    ********************Creating admin account
    <CreateAccountRequest accountName:"admin-account1388526489", password:"123456", accountType:Admin (2)>
    <CreateAccountResponse accountId:2831469159201232211, accountName:"admin-account1388526489">
    ********************Creating regular account1
    <CreateAccountRequest accountName:"regular-account11388526489", password:"123456", accountType:Regular (3)>
    <CreateAccountResponse accountId:4495195272100583936, accountName:"regular-account11388526489">
    ********************Creating regular account2
    <CreateAccountRequest accountName:"regular-account21388526489", password:"123456", accountType:Regular (3)>
    <CreateAccountResponse accountId:654124590932692900, accountName:"regular-account21388526489">
    ********************Listing accounts with admin account
    <ListAccountsRequest accountId:2831469159201232211>
    <ListAccountsResponse accounts:[<AccountMetadataThrift accountId:3313661573649971201, accountName:"account1388526391822", accountType:Regular (3)>, <AccountMetadataThrift accountId:1998661244848372247, accountName:"super-admin-account1388526489", accountType:SuperAdmin (1)>, <AccountMetadataThrift accountId:2831469159201232211, accountName:"admin-account1388526489", accountType:Admin (2)>, <AccountMetadataThrift accountId:4495195272100583936, accountName:"regular-account11388526489", accountType:Regular (3)>, <AccountMetadataThrift accountId:654124590932692900, accountName:"regular-account21388526489", accountType:Regular (3)>]>
    ********************Listing accounts with regular account, expect to get AccessDeniedException
    <ListAccountsRequest accountId:4495195272100583936>
    AccessDeniedExceptionThrift
    ********************Authenticating regular account
    <AuthenticateAccountRequest accountName:"regular-account11388526489", password:"123456">
    <AuthenticateAccountResponse accountId:4495195272100583936, accountName:"regular-account11388526489">
    ********************Authenticating regular account with wrong password, expect to get Exception
    <AuthenticateAccountRequest accountName:"regular-account11388526489", password:"123456wrong">
    AuthenticationFailedExceptionThrift
    ********************Creating volume with regular account
    <CreateVolumeRequest requestId:nil, volumeSize:16777216, name:"vol1388526489", volumeType:REGULAR (1), accountId:4495195272100583936>
    <CreateVolumeResponse requestId:0, volumeId:1454120426141387055>
    ********************Sleeping 60s
    ********************Listing volumes with regualr account, expect to get back 1 volume
    <ListVolumesRequest requestId:nil, accountId:4495195272100583936>
    <ListVolumesResponse requestId:0, volumes:[<VolumeMetadataThrift volumeId:1454120426141387055, name:"vol1388526489", endpoint:nil, volumeSize:16777216, volumeType:REGULAR (1), volumeStatus:Available (2), accountId:4495195272100583936>]>
    ********************Listing volumes with admin account, expect to get back more than 1 volume
    <ListVolumesRequest requestId:nil, accountId:2831469159201232211>
    <ListVolumesResponse requestId:0, volumes:[<VolumeMetadataThrift volumeId:1454120426141387055, name:"vol1388526489", endpoint:nil, volumeSize:16777216, volumeType:REGULAR (1), volumeStatus:Available (2), accountId:4495195272100583936>, <VolumeMetadataThrift volumeId:1902150920790493123, name:"testvolume", endpoint:nil, volumeSize:16777216, volumeType:REGULAR (1), volumeStatus:Available (2), accountId:3313661573649971201>]>
    ********************Getting volume with regular account
    <GetVolumeRequest requestId:nil, volumeId:1454120426141387055, accountId:4495195272100583936>
    <GetVolumeResponse requestId:0, volumeMetadata:<VolumeMetadataThrift volumeId:1454120426141387055, name:"vol1388526489", endpoint:nil, volumeSize:16777216, volumeType:REGULAR (1), volumeStatus:Available (2), accountId:4495195272100583936>, segmentsMetadata:[<SegmentMetadataThrift volumeId:1454120426141387055, segIndex:0, segmentUnits:[<SegmentUnitMetadataThrift volumeId:1454120426141387055, segIndex:0, offset:25298944, epoch:2, generation:0, status:Secondary (7), primary:10011, secondaries:[10013, 10012], volumeType:"REGULAR", instanceId:10012>, <SegmentUnitMetadataThrift volumeId:1454120426141387055, segIndex:0, offset:25298944, epoch:2, generation:0, status:Primary (8), primary:10011, secondaries:[10013, 10012], volumeType:"REGULAR", instanceId:10011>]>]>
    ********************Getting volume with admin account, which is ok
    <GetVolumeRequest requestId:nil, volumeId:1454120426141387055, accountId:2831469159201232211>
    <GetVolumeResponse requestId:0, volumeMetadata:<VolumeMetadataThrift volumeId:1454120426141387055, name:"vol1388526489", endpoint:nil, volumeSize:16777216, volumeType:REGULAR (1), volumeStatus:Available (2), accountId:4495195272100583936>, segmentsMetadata:[<SegmentMetadataThrift volumeId:1454120426141387055, segIndex:0, segmentUnits:[<SegmentUnitMetadataThrift volumeId:1454120426141387055, segIndex:0, offset:25298944, epoch:2, generation:0, status:Secondary (7), primary:10011, secondaries:[10013, 10012], volumeType:"REGULAR", instanceId:10012>, <SegmentUnitMetadataThrift volumeId:1454120426141387055, segIndex:0, offset:25298944, epoch:2, generation:0, status:Primary (8), primary:10011, secondaries:[10013, 10012], volumeType:"REGULAR", instanceId:10011>]>]>
    ********************Getting volume with regualr account2, expect to get AccessDeniedException
    <GetVolumeRequest requestId:nil, volumeId:1454120426141387055, accountId:654124590932692900>
    AccessDeniedExceptionThrift
    ********************Deleting volume (created by regualr account 1) with regular account2, expect to get AccessDeniedException
    <DeleteVolumeRequest requestId:nil, volumeId:1454120426141387055, accountId:654124590932692900>
    AccessDeniedExceptionThrift
    ********************Deleting volume (created by regualr account 1) with regular account1
    <DeleteVolumeRequest requestId:nil, volumeId:1454120426141387055, accountId:4495195272100583936>
    <DeleteVolumeResponse requestId:0>
    ********************Sleeping 60s
    ********************Getting volume again with regular account
    <GetVolumeRequest requestId:nil, volumeId:1454120426141387055, accountId:4495195272100583936>
    <GetVolumeResponse requestId:0, volumeMetadata:<VolumeMetadataThrift volumeId:1454120426141387055, name:"vol1388526489", endpoint:nil, volumeSize:16777216, volumeType:REGULAR (1), volumeStatus:Unavailable (3), accountId:4495195272100583936>, segmentsMetadata:[<SegmentMetadataThrift volumeId:1454120426141387055, segIndex:0, segmentUnits:[<SegmentUnitMetadataThrift volumeId:1454120426141387055, segIndex:0, offset:25298944, epoch:2, generation:0, status:Deleting (9), primary:10011, secondaries:[10013, 10012], volumeType:"REGULAR", instanceId:10012>, <SegmentUnitMetadataThrift volumeId:1454120426141387055, segIndex:0, offset:25298944, epoch:2, generation:0, status:Secondary (7), primary:10011, secondaries:[10013, 10012], volumeType:"REGULAR", instanceId:10013>, <SegmentUnitMetadataThrift volumeId:1454120426141387055, segIndex:0, offset:25298944, epoch:2, generation:0, status:Deleting (9), primary:10011, secondaries:[10013, 10012], volumeType:"REGULAR", instanceId:10011>]>]>