
 load patches;
T1=lsh('lsh',10,24,size(patches,1),patches,'range',255)
% 


% %tic
% %lshstats(T1(1:10),'test',patches,patches(:,100:100:1000),4)
% %toc
% 
 dataapp=zeros(1,400)
 dataexh=zeros(1,400)
 datareal=zeros(1,400)
% %Search the structure for the 3 nearest neighbors under l1
% 
times=zeros(1,10)
times2=zeros(1,10)
%%%
for z = 100:100:1000
    for i = 1:20
    tic; [nnlsh,numcand]=lshlookup(patches(:,z),patches,T1,'k',4,'distfun','lpnorm','distargs',{1});toc;
    times(z/100,i) = toc;
    end
end

%exhaustive search
for z = 100:100:1000
    for i = 1:20
    tic;d=sum(abs(bsxfun(@minus,patches(:,z),patches)));
    [ignore,ind]=sort(d);toc;
    times2(z/100,i) = toc;
    end
end

sum(sum(times,2)/20)
sum(sum(times2,2)/20)

%%

% for z = 100:100:1000
%     
%     tic; [nnlsh,numcand]=lshlookup(patches(:,z),patches,T1,'k',4,'distfun','lpnorm','distargs',{1});toc;
%     times(z/100,i) = toc;
%     
%     while size(patches(:,nnlsh),2) ~= 4
%             tic; [nnlsh,numcand]=lshlookup(patches(:,z),patches,T1,'k',4,'distfun','lpnorm','distargs',{1});toc;
%     end
%         
%       
%       datareal(z/100,:)=patches(:,nnlsh(1))';
%       if z==100
%       dataapp(1,:)=patches(:,nnlsh(2))';
%       dataapp=[dataapp;patches(:,nnlsh(3))'];
%       dataapp=[dataapp;patches(:,nnlsh(4))'];
%       else
%       dataapp=[dataapp;patches(:,nnlsh(2))'];
%       dataapp=[dataapp;patches(:,nnlsh(3))'];
%       dataapp=[dataapp;patches(:,nnlsh(4))'];
%       end
% end
% 
% % exhaustive search
% for z = 100:100:1000
%     d=sum(abs(bsxfun(@minus,patches(:,z),patches)));
%     [ignore,ind]=sort(d);
% 
%        if z==100
%        dataexh(1,:)=patches(:, ind(2))';
%        dataexh=[dataexh;patches(:,ind(3))'];
%        dataexh=[dataexh;patches(:,ind(4))'];
%       else
%       dataexh=[dataexh;patches(:,ind(2))'];
%       dataexh=[dataexh;patches(:,ind(3))'];
%       dataexh=[dataexh;patches(:,ind(4))'];
%        end
% % 
%  end
% 
% % %% Manhattan distance - L1
% % got help with repeating rows online %
%  datareal=kron(datareal,ones(3,1))
% 
%  distancesapp=sum(abs(bsxfun(@minus,datareal,dataapp)),2)
%  distancesexh=sum(abs(bsxfun(@minus,datareal,dataexh)),2)
% % 
% 
% distancesapp=reshape(distancesapp,3,10)
% sumdisapp=sum(distancesapp)
% 
% distancesexh=reshape(distancesexh,3,10)
% sumdisexh=sum(distancesexh)
% 
% divsum=sumdisapp./sumdisexh
% totsum=sum(divsum)
% final = 1/10*totsum


% load patches;
% %T1=lsh('lsh',10,24,size(patches,1),patches,'range',255)
% 


% %tic
% %lshstats(T1(1:10),'test',patches,patches(:,100:100:1000),4)
% %toc
% 
 dataapp=zeros(1,400);
 dataexh=zeros(1,400);
 datareal=zeros(1,400);
% %Search the structure for the 3 nearest neighbors under l1
% 
% times=zeros(1,10)
% times2=zeros(1,10)
% %%%
% for z = 100:100:1000
%     for i = 1:20
%     tic; [nnlsh,numcand]=lshlookup(patches(:,z),patches,T1,'k',4,'distfun','lpnorm','distargs',{1});toc;
%     times(z/100,i) = toc;
%     end
% end
% 
% %exhaustive search
% for z = 100:100:1000
%     for i = 1:20
%     tic;d=sum(abs(bsxfun(@minus,patches(:,z),patches)));
%     [ignore,ind]=sort(d);toc;
%     times2(z/100,i) = toc;
%     end
% end
% 
% sum(sum(times,2)/20)
% sum(sum(times2,2)/20)

%%
errorL =zeros(1,6)
errork =zeros(1,5)

count = 0

for L = 10:2:20
    L
    dataapp=zeros(1,400);
    dataexh=zeros(1,400);
    datareal=zeros(1,400);
    count = count +1
    T1=lsh('lsh',L,24,size(patches,1),patches,'range',255);

    for z = 100:100:1000
    
        [nnlsh,numcand]=lshlookup(patches(:,z),patches,T1,'k',4,'distfun','lpnorm','distargs',{1});
    
        while size(patches(:,nnlsh),2) ~= 4
            [nnlsh,numcand]=lshlookup(patches(:,z),patches,T1,'k',4,'distfun','lpnorm','distargs',{1});
        end
        
      
        datareal(z/100,:)=patches(:,nnlsh(1))';
        if z==100
            dataapp(1,:)=patches(:,nnlsh(2))';
            dataapp=[dataapp;patches(:,nnlsh(3))'];
            dataapp=[dataapp;patches(:,nnlsh(4))'];
        else
            dataapp=[dataapp;patches(:,nnlsh(2))'];
            dataapp=[dataapp;patches(:,nnlsh(3))'];
            dataapp=[dataapp;patches(:,nnlsh(4))'];
        end
    end

% exhaustive search
    for z = 100:100:1000
        d=sum(abs(bsxfun(@minus,patches(:,z),patches)));
        [ignore,ind]=sort(d);

        if z==100
            dataexh(1,:)=patches(:, ind(2))';
            dataexh=[dataexh;patches(:,ind(3))'];
            dataexh=[dataexh;patches(:,ind(4))'];
        else
            dataexh=[dataexh;patches(:,ind(2))'];
            dataexh=[dataexh;patches(:,ind(3))'];
            dataexh=[dataexh;patches(:,ind(4))'];
        end
% 
    end

% %% Manhattan distance - L1
% got help with repeating rows online %
    datareal=kron(datareal,ones(3,1));

    distancesapp=sum(abs(bsxfun(@minus,datareal,dataapp)),2);
    distancesexh=sum(abs(bsxfun(@minus,datareal,dataexh)),2);
% 

    distancesapp=reshape(distancesapp,3,10);
    sumdisapp=sum(distancesapp);

    distancesexh=reshape(distancesexh,3,10);
    sumdisexh=sum(distancesexh);

    divsum=sumdisapp./sumdisexh;
    totsum=sum(divsum);
    final = 1/10*totsum
    errorL(1,count) = final
end

count = 0

for K = 16:2:24
    K
    dataapp=zeros(1,400);
    dataexh=zeros(1,400);
    datareal=zeros(1,400);
    count = count +1
    T1=lsh('lsh',10,K,size(patches,1),patches,'range',255);

    for z = 100:100:1000
    
        [nnlsh,numcand]=lshlookup(patches(:,z),patches,T1,'k',4,'distfun','lpnorm','distargs',{1});
    
        while size(patches(:,nnlsh),2) ~= 4
            [nnlsh,numcand]=lshlookup(patches(:,z),patches,T1,'k',4,'distfun','lpnorm','distargs',{1});
        end
        
      
        datareal(z/100,:)=patches(:,nnlsh(1))';
        if z==100
            dataapp(1,:)=patches(:,nnlsh(2))';
            dataapp=[dataapp;patches(:,nnlsh(3))'];
            dataapp=[dataapp;patches(:,nnlsh(4))'];
        else
            dataapp=[dataapp;patches(:,nnlsh(2))'];
            dataapp=[dataapp;patches(:,nnlsh(3))'];
            dataapp=[dataapp;patches(:,nnlsh(4))'];
        end
    end

% exhaustive search
    for z = 100:100:1000
        d=sum(abs(bsxfun(@minus,patches(:,z),patches)));
        [ignore,ind]=sort(d);

        if z==100
            dataexh(1,:)=patches(:, ind(2))';
            dataexh=[dataexh;patches(:,ind(3))'];
            dataexh=[dataexh;patches(:,ind(4))'];
        else
            dataexh=[dataexh;patches(:,ind(2))'];
            dataexh=[dataexh;patches(:,ind(3))'];
            dataexh=[dataexh;patches(:,ind(4))'];
        end
% 
    end

% %% Manhattan distance - L1
% got help with repeating rows online %
    datareal=kron(datareal,ones(3,1));

    distancesapp=sum(abs(bsxfun(@minus,datareal,dataapp)),2);
    distancesexh=sum(abs(bsxfun(@minus,datareal,dataexh)),2);
% 

    distancesapp=reshape(distancesapp,3,10);
    sumdisapp=sum(distancesapp);

    distancesexh=reshape(distancesexh,3,10);
    sumdisexh=sum(distancesexh);

    divsum=sumdisapp./sumdisexh;
    totsum=sum(divsum);
    final = 1/10*totsum
    errork(1,count) = final
end

figure(1)

subplot(1,2,1)
plot(10:2:20,errorL,'k-o')
xlabel('L')
ylabel('Error')
title('Error as a function of L')

subplot(1,2,2)
plot(16:2:24,errork,'k-o')
xlabel('k')
ylabel('Error')
title('Error as a function of k')


clear all

load patches;
T1=lsh('lsh',10,24,size(patches,1),patches,'range',255)

[nnlsh,numcand]=lshlookup(patches(:,100),patches,T1,'k',11,'distfun','lpnorm','distargs',{1});

figure(2); clf;

imagesc(reshape(patches(:,nnlsh(1)),20,20)); colormap gray;axis image;
title("Image 100")

figure(3);clf;
for k=1:10, subplot(2,5,k);imagesc(reshape(patches(:,nnlsh(k+1)),20,20)); colormap gray;axis image; end

d=sum(abs(bsxfun(@minus,patches(:,100),patches)));
[ignore,ind]=sort(d);
title('Nearest neighbors using LSH')

figure(4);clf;
for k=1:10, subplot(2,5,k);imagesc(reshape(patches(:,ind(k+1)),20,20));colormap gray;axis image; end
title('Nearest Neighbors using Exhaustive')