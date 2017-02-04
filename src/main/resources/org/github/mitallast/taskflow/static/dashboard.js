(function(){
    angular.module('dag', [])
    .controller('dag-latest-ctrl', function($scope, $http){
        $http.get('/api/dag/latest')
        .then(function(response){
            $scope.latest = response.data;
        });
    })
    .controller('dag-id-ctrl', function($scope, $http){
        $http.get('/api/dag/id/4')
        .then(function(response){
            $scope.dag = response.data;
        });
    })
    .controller('dag-run-ctrl', function($scope, $http){
        $http.get('/api/dag/run')
        .then(function(response){
            $scope.runs = response.data;
        });
    })
    .controller('dag-run-pending-ctrl', function($scope, $http){
        $http.get('/api/dag/run/pending')
        .then(function(response){
            $scope.runs = response.data;
        });
    })
    .controller('dag-run-id-ctrl', function($scope, $http){
        $http.get('/api/dag/run/id/1')
        .then(function(response){
            $scope.dag_run = response.data;
        });
    });
})();