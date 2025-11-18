targetScope = 'subscription'

@minLength(1)
@maxLength(64)
@description('Name of the the environment which is used to generate a short unique hash used in all resources.')
param environmentName string

@minLength(1)
@description('Primary location for all resources')
// microsoft.insights/components has restricted regions
@allowed([
  'eastus'
  'southcentralus'
  'northeurope'
  'westeurope'
  'southeastasia'
  'westus2'
  'uksouth'
  'canadacentral'
  'centralindia'
  'japaneast'
  'australiaeast'
  'koreacentral'
  'francecentral'
  'centralus'
  'eastus2'
  'eastasia'
  'westus'
  'southafricanorth'
  'northcentralus'
  'brazilsouth'
  'switzerlandnorth'
  'norwayeast'
  'norwaywest'
  'australiasoutheast'
  'australiacentral2'
  'germanywestcentral'
  'switzerlandwest'
  'uaecentral'
  'ukwest'
  'japanwest'
  'brazilsoutheast'
  'uaenorth'
  'australiacentral'
  'southindia'
  'westus3'
  'koreasouth'
  'swedencentral'
  'canadaeast'
  'jioindiacentral'
  'jioindiawest'
  'qatarcentral'
  'southafricawest'
  'germanynorth'
  'polandcentral'
  'israelcentral'
  'italynorth'
  'mexicocentral'
  'spaincentral'
  'newzealandnorth'
  'chilecentral'
  'indonesiacentral'
  'malaysiawest'
])
@metadata({
  azd: {
    type: 'location'
  }
})
param location string

param backendServiceName string = '' // Set in main.parameters.json
param resourceGroupName string = '' // Set in main.parameters.json

param applicationInsightsDashboardName string = '' // Set in main.parameters.json
param applicationInsightsName string = '' // Set in main.parameters.json
param logAnalyticsName string = '' // Set in main.parameters.json

param storageAccountName string = '' // Set in main.parameters.json
param storageResourceGroupName string = '' // Set in main.parameters.json
param storageResourceGroupLocation string = location
param storageContainerName string = 'content'
param storageSkuName string // Set in main.parameters.json

param defaultReasoningEffort string // Set in main.parameters.json
param useAgenticRetrieval bool // Set in main.parameters.json

param userStorageAccountName string = ''
param userStorageContainerName string = 'user-content'

param tokenStorageContainerName string = 'tokens'

param imageStorageContainerName string = 'images'

param useEval bool = false

param tenantId string = tenant().tenantId
param authTenantId string = ''

// Used for the optional login and document level access control system
param useAuthentication bool = false
param enforceAccessControl bool = false
param enableGlobalDocuments bool = false
param enableUnauthenticatedAccess bool = false
param serverAppId string = ''
@secure()
param serverAppSecret string = ''
param clientAppId string = ''
@secure()
param clientAppSecret string = ''

// Used for optional CORS support for alternate frontends
param allowedOrigin string = '' // should start with https://, shouldn't end with a /

@allowed(['None', 'AzureServices'])
@description('If allowedIp is set, whether azure services are allowed to bypass the storage and AI services firewall.')
param bypass string = 'AzureServices'

@description('Public network access value for all deployed resources')
@allowed(['Enabled', 'Disabled'])
param publicNetworkAccess string = 'Enabled'

@description('Add a private endpoints for network connectivity')
param usePrivateEndpoint bool = false

@description('Use a P2S VPN Gateway for secure access to the private endpoints')
param useVpnGateway bool = false

@description('Id of the user or app to assign application roles')
param principalId string = ''

@description('Use Application Insights for monitoring and performance tracing')
param useApplicationInsights bool = false

var abbrs = loadJsonContent('abbreviations.json')
var resourceToken = toLower(uniqueString(subscription().id, environmentName, location))
var tags = { 'azd-env-name': environmentName }

var tenantIdForAuth = !empty(authTenantId) ? authTenantId : tenantId
var authenticationIssuerUri = '${environment().authentication.loginEndpoint}${tenantIdForAuth}/v2.0'

@description('Whether the deployment is running on GitHub Actions')
param runningOnGh string = ''

@description('Whether the deployment is running on Azure DevOps Pipeline')
param runningOnAdo string = ''

param webAppExists bool

@allowed(['Consumption', 'D4', 'D8', 'D16', 'D32', 'E4', 'E8', 'E16', 'E32', 'NC24-A100', 'NC48-A100', 'NC96-A100'])
param azureContainerAppsWorkloadProfile string

param acaIdentityName string = '${environmentName}-aca-identity'
param acaManagedEnvironmentName string = '${environmentName}-aca-env'
param containerRegistryName string = '${replace(toLower(environmentName), '-', '')}acr'

// Configure CORS for allowing different web apps to use the backend
// For more information please see https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS
var msftAllowedOrigins = ['https://portal.azure.com', 'https://ms.portal.azure.com']
var loginEndpoint = environment().authentication.loginEndpoint
var loginEndpointFixed = lastIndexOf(loginEndpoint, '/') == length(loginEndpoint) - 1
  ? substring(loginEndpoint, 0, length(loginEndpoint) - 1)
  : loginEndpoint
var allMsftAllowedOrigins = !(empty(clientAppId)) ? union(msftAllowedOrigins, [loginEndpointFixed]) : msftAllowedOrigins
// Combine custom origins with Microsoft origins, remove any empty origin strings and remove any duplicate origins
var allowedOrigins = reduce(
  filter(union(split(allowedOrigin, ';'), allMsftAllowedOrigins), o => length(trim(o)) > 0),
  [],
  (cur, next) => union(cur, [next])
)

// Organize resources in a resource group
resource resourceGroup 'Microsoft.Resources/resourceGroups@2024-11-01' existing = {
  name: !empty(resourceGroupName) ? resourceGroupName : '${abbrs.resourcesResourceGroups}${environmentName}'
  scope: subscription()
}

resource applicationInsightsDashboard 'Microsoft.Portal/dashboards@2020-09-01-preview' = if (useApplicationInsights) {
  name: applicationInsightsDashboardName
  location: location
  tags: tags
}

resource storageResourceGroup 'Microsoft.Resources/resourceGroups@2024-11-01' existing = if (!empty(storageResourceGroupName)) {
  name: !empty(storageResourceGroupName) ? storageResourceGroupName : resourceGroup.name
}

// Monitor application with Azure Monitor
module monitoring 'core/monitor/monitoring.bicep' = if (useApplicationInsights) {
  name: 'monitoring'
  scope: resourceGroup
  params: {
    location: location
    tags: tags
    applicationInsightsName: !empty(applicationInsightsName)
      ? applicationInsightsName
      : '${abbrs.insightsComponents}${resourceToken}'
    logAnalyticsName: !empty(logAnalyticsName)
      ? logAnalyticsName
      : '${abbrs.operationalInsightsWorkspaces}${resourceToken}'
    publicNetworkAccess: publicNetworkAccess
  }
}

module applicationInsightsDashboard 'backend-dashboard.bicep' = if (useApplicationInsights) {
  name: 'application-insights-dashboard'
  scope: resourceGroup
  params: {
    name: !empty(applicationInsightsDashboardName)
      ? applicationInsightsDashboardName
      : '${abbrs.portalDashboards}${resourceToken}'
    location: location
    applicationInsightsName: useApplicationInsights ? monitoring.outputs.applicationInsightsName : ''
  }
}

var appEnvVariables = {
  AZURE_STORAGE_ACCOUNT: storage.outputs.name
  AZURE_STORAGE_CONTAINER: storageContainerName
  APPLICATIONINSIGHTS_CONNECTION_STRING: useApplicationInsights
    ? monitoring.outputs.applicationInsightsConnectionString
    : ''

  // Optional login and document level access control system
  AZURE_USE_AUTHENTICATION: useAuthentication
  AZURE_ENFORCE_ACCESS_CONTROL: enforceAccessControl
  AZURE_ENABLE_GLOBAL_DOCUMENT_ACCESS: enableGlobalDocuments
  AZURE_ENABLE_UNAUTHENTICATED_ACCESS: enableUnauthenticatedAccess
  AZURE_SERVER_APP_ID: serverAppId
  AZURE_CLIENT_APP_ID: clientAppId
  AZURE_TENANT_ID: tenantId
  AZURE_AUTH_TENANT_ID: tenantIdForAuth
  AZURE_AUTHENTICATION_ISSUER_URI: authenticationIssuerUri
  // CORS support, for frontends on other hosts
  ALLOWED_ORIGIN: join(allowedOrigins, ';')
  AZURE_USERSTORAGE_ACCOUNT: useUserUpload ? userStorage.outputs.name : ''
  AZURE_USERSTORAGE_CONTAINER: useUserUpload ? userStorageContainerName : ''
  AZURE_IMAGESTORAGE_CONTAINER: useMultimodal ? imageStorageContainerName : ''
  RUNNING_IN_PRODUCTION: 'true'
}

// Azure container apps resources

// User-assigned identity for pulling images from ACR
module acaIdentity 'core/security/aca-identity.bicep' = {
  name: 'aca-identity'
  scope: resourceGroup
  params: {
    identityName: acaIdentityName
    location: location
  }
}

module containerApps 'core/host/container-apps.bicep' = {
  name: 'container-apps'
  scope: resourceGroup
  params: {
    name: 'app'
    tags: tags
    location: location
    containerAppsEnvironmentName: acaManagedEnvironmentName
    containerRegistryName: '${containerRegistryName}${resourceToken}'
    logAnalyticsWorkspaceName: useApplicationInsights ? monitoring.outputs.logAnalyticsWorkspaceName : ''
    subnetResourceId: usePrivateEndpoint ? isolation.outputs.appSubnetId : ''
    usePrivateIngress: usePrivateEndpoint
    workloadProfile: azureContainerAppsWorkloadProfile
  }
}

// Container Apps for the web application (Python Quart app with JS frontend)
module acaBackend 'core/host/container-app-upsert.bicep' = {
  name: 'aca-web'
  scope: resourceGroup
  params: {
    name: !empty(backendServiceName) ? backendServiceName : '${abbrs.webSitesContainerApps}backend-${resourceToken}'
    location: location
    identityName: acaIdentityName
    exists: webAppExists
    containerRegistryName: containerApps.outputs.registryName
    containerAppsEnvironmentName: containerApps.outputs.environmentName
    tags: union(tags, { 'azd-service-name': 'backend' })
    targetPort: 8000
    containerCpuCoreCount: '1.0'
    containerMemory: '2Gi'
    containerMinReplicas: usePrivateEndpoint ? 1 : 0
    allowedOrigins: allowedOrigins
    env: union(appEnvVariables, {
      // For using managed identity to access Azure resources. See https://github.com/microsoft/azure-container-apps/issues/442
      AZURE_CLIENT_ID: acaIdentity.outputs.clientId
    })
    secrets: useAuthentication
      ? {
          azureclientappsecret: clientAppSecret
          azureserverappsecret: serverAppSecret
        }
      : {}
    envSecrets: useAuthentication
      ? [
          {
            name: 'AZURE_CLIENT_APP_SECRET'
            secretRef: 'azureclientappsecret'
          }
          {
            name: 'AZURE_SERVER_APP_SECRET'
            secretRef: 'azureserverappsecret'
          }
        ]
      : []
  }
}

module acaAuth 'core/host/container-apps-auth.bicep' = if (!empty(clientAppId)) {
  name: 'aca-auth'
  scope: resourceGroup
  params: {
    name: acaBackend.outputs.name
    clientAppId: clientAppId
    serverAppId: serverAppId
    clientSecretSettingName: !empty(clientAppSecret) ? 'azureclientappsecret' : ''
    authenticationIssuerUri: authenticationIssuerUri
    enableUnauthenticatedAccess: enableUnauthenticatedAccess
    blobContainerUri: 'https://${storageAccountName}.blob.${environment().suffixes.storage}/${tokenStorageContainerName}'
    appIdentityResourceId: acaBackend.outputs.identityResourceId
  }
}

module storage 'core/storage/storage-account.bicep' = {
  name: 'storage'
  scope: storageResourceGroup
  params: {
    name: !empty(storageAccountName) ? storageAccountName : '${abbrs.storageStorageAccounts}${resourceToken}'
    location: storageResourceGroupLocation
    tags: tags
    publicNetworkAccess: publicNetworkAccess
    bypass: bypass
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    sku: {
      name: storageSkuName
    }
    deleteRetentionPolicy: {
      enabled: true
      days: 2
    }
    containers: [
      {
        name: storageContainerName
        publicAccess: 'None'
      }
      {
        name: imageStorageContainerName
        publicAccess: 'None'
      }
      {
        name: tokenStorageContainerName
        publicAccess: 'None'
      }
    ]
  }
}

module userStorage 'core/storage/storage-account.bicep' = if (useUserUpload) {
  name: 'user-storage'
  scope: storageResourceGroup
  params: {
    name: !empty(userStorageAccountName)
      ? userStorageAccountName
      : 'user${abbrs.storageStorageAccounts}${resourceToken}'
    location: storageResourceGroupLocation
    tags: tags
    publicNetworkAccess: publicNetworkAccess
    bypass: bypass
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    isHnsEnabled: true
    sku: {
      name: storageSkuName
    }
    containers: [
      {
        name: userStorageContainerName
        publicAccess: 'None'
      }
    ]
  }
}

module ai 'core/ai/ai-environment.bicep' = if (useAiProject) {
  name: 'ai'
  scope: resourceGroup
  params: {
    // Limited region support: https://learn.microsoft.com/azure/ai-foundry/how-to/develop/evaluate-sdk#region-support
    location: 'eastus2'
    tags: tags
    hubName: 'aihub-${resourceToken}'
    projectName: 'aiproj-${resourceToken}'
    storageAccountId: storage.outputs.id
    applicationInsightsId: !useApplicationInsights ? '' : monitoring.outputs.applicationInsightsId
  }
}

// USER ROLES
var principalType = empty(runningOnGh) && empty(runningOnAdo) ? 'User' : 'ServicePrincipal'

module speechRoleUser 'core/security/role.bicep' = {
  scope: speechResourceGroup
  name: 'speech-role-user'
  params: {
    principalId: principalId
    roleDefinitionId: 'f2dc8367-1007-4938-bd23-fe263f013447'
    principalType: principalType
  }
}

module storageRoleUser 'core/security/role.bicep' = {
  scope: storageResourceGroup
  name: 'storage-role-user'
  params: {
    principalId: principalId
    roleDefinitionId: '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1' // Storage Blob Data Reader
    principalType: principalType
  }
}

module storageContribRoleUser 'core/security/role.bicep' = {
  scope: storageResourceGroup
  name: 'storage-contrib-role-user'
  params: {
    principalId: principalId
    roleDefinitionId: 'ba92f5b4-2d11-453d-a403-e96b0029c9fe' // Storage Blob Data Contributor
    principalType: principalType
  }
}

module storageOwnerRoleUser 'core/security/role.bicep' = if (useUserUpload) {
  scope: storageResourceGroup
  name: 'storage-owner-role-user'
  params: {
    principalId: principalId
    roleDefinitionId: 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b' // Storage Blob Data Owner
    principalType: principalType
  }
}

// SYSTEM IDENTITIES
module storageRoleBackend 'core/security/role.bicep' = {
  scope: storageResourceGroup
  name: 'storage-role-backend'
  params: {
    principalId: acaBackend.outputs.identityPrincipalId
    roleDefinitionId: '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1' // Storage Blob Data Reader
    principalType: 'ServicePrincipal'
  }
}

module storageOwnerRoleBackend 'core/security/role.bicep' = if (useUserUpload) {
  scope: storageResourceGroup
  name: 'storage-owner-role-backend'
  params: {
    principalId: acaBackend.outputs.identityPrincipalId
    roleDefinitionId: 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b' // Storage Blob Data Owner
    principalType: 'ServicePrincipal'
  }
}

// Necessary for the Container Apps backend to store authentication tokens in the blob storage container
module storageRoleContributorBackend 'core/security/role.bicep' = if (!empty(clientAppId)) {
  scope: storageResourceGroup
  name: 'storage-role-contributor-aca-backend'
  params: {
    principalId: acaBackend.outputs.identityPrincipalId
    roleDefinitionId: 'ba92f5b4-2d11-453d-a403-e96b0029c9fe' // Storage Blob Data Contributor
    principalType: 'ServicePrincipal'
  }
}

module speechRoleBackend 'core/security/role.bicep' = {
  scope: speechResourceGroup
  name: 'speech-role-backend'
  params: {
    principalId: acaBackend.outputs.identityPrincipalId
    roleDefinitionId: 'f2dc8367-1007-4938-bd23-fe263f013447'
    principalType: 'ServicePrincipal'
  }
}

module isolation 'network-isolation.bicep' = if (usePrivateEndpoint) {
  name: 'networks'
  scope: resourceGroup
  params: {
    location: location
    tags: tags
    vnetName: '${abbrs.virtualNetworks}${resourceToken}'
    deploymentTarget: 'containerapps'
    useVpnGateway: useVpnGateway
    vpnGatewayName: useVpnGateway ? '${abbrs.networkVpnGateways}${resourceToken}' : ''
    dnsResolverName: useVpnGateway ? '${abbrs.privateDnsResolver}${resourceToken}' : ''
  }
}

var environmentData = environment()

var containerAppsPrivateEndpointConnection = usePrivateEndpoint
  ? [
      {
        groupId: 'managedEnvironments'
        dnsZoneName: 'privatelink.${location}.azurecontainerapps.io'
        resourceIds: [containerApps.outputs.environmentId]
      }
    ]
  : []

var otherPrivateEndpointConnections = (usePrivateEndpoint)
  ? [
      {
        groupId: 'blob'
        dnsZoneName: 'privatelink.blob.${environmentData.suffixes.storage}'
        resourceIds: concat([storage.outputs.id], useUserUpload ? [userStorage.outputs.id] : [])
      }
    ]
  : []

var privateEndpointConnections = concat(
  cognitiveServicesPrivateEndpointConnection,
  containerAppsPrivateEndpointConnection,
  otherPrivateEndpointConnections
)

module privateEndpoints 'private-endpoints.bicep' = if (usePrivateEndpoint) {
  name: 'privateEndpoints'
  scope: resourceGroup
  params: {
    location: location
    tags: tags
    resourceToken: resourceToken
    privateEndpointConnections: privateEndpointConnections
    applicationInsightsId: useApplicationInsights ? monitoring.outputs.applicationInsightsId : ''
    logAnalyticsWorkspaceId: useApplicationInsights ? monitoring.outputs.logAnalyticsWorkspaceId : ''
    vnetName: isolation.outputs.vnetName
    vnetPeSubnetId: isolation.outputs.backendSubnetId
  }
}

output AZURE_LOCATION string = location
output AZURE_TENANT_ID string = tenantId
output AZURE_AUTH_TENANT_ID string = authTenantId
output AZURE_RESOURCE_GROUP string = resourceGroup.name

output AZURE_STORAGE_ACCOUNT string = storage.outputs.name
output AZURE_STORAGE_CONTAINER string = storageContainerName
output AZURE_STORAGE_RESOURCE_GROUP string = storageResourceGroup.name

output AZURE_USERSTORAGE_ACCOUNT string = useUserUpload ? userStorage.outputs.name : ''
output AZURE_USERSTORAGE_CONTAINER string = userStorageContainerName
output AZURE_USERSTORAGE_RESOURCE_GROUP string = storageResourceGroup.name

output AZURE_IMAGESTORAGE_CONTAINER string = useMultimodal ? imageStorageContainerName : ''

output AZURE_USE_AUTHENTICATION bool = useAuthentication

output BACKEND_URI string = acaBackend.outputs.uri
output AZURE_CONTAINER_REGISTRY_ENDPOINT string = containerApps.outputs.registryLoginServer

output AZURE_VPN_CONFIG_DOWNLOAD_LINK string = useVpnGateway
  ? 'https://portal.azure.com/#@${tenant().tenantId}/resource${isolation.outputs.virtualNetworkGatewayId}/pointtositeconfiguration'
  : ''
