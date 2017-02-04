(function(){
    angular.module('dag', [])
    .controller('dagCtrl', function($scope, $http){
        $http.get('/api/dag/latest')
        .then(function(success){
            $scope.latest = success.data;
        }, function(error){
            $scope.error = error;
        });
    });
})();